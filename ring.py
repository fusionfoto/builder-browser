# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import math
import random
import cPickle as pickle
from copy import deepcopy
from contextlib import contextmanager

from collections import defaultdict
from time import time


def tiers_for_dev(dev):
    """
    Returns a tuple of tiers for a given device in ascending order by
    length.

    :returns: tuple of tiers
    """
    t1 = dev['region']
    t2 = dev['zone']
    t3 = dev['ip']
    t4 = dev['id']

    return ((t1,),
            (t1, t2),
            (t1, t2, t3),
            (t1, t2, t3, t4))


def build_tier_tree(devices):
    """
    Construct the tier tree from the zone layout.

    The tier tree is a dictionary that maps tiers to their child tiers.
    A synthetic root node of () is generated so that there's one tree,
    not a forest.

    Example:

    region 1 -+---- zone 1 -+---- 192.168.101.1 -+---- device id 0
              |             |                    |
              |             |                    +---- device id 1
              |             |                    |
              |             |                    +---- device id 2
              |             |
              |             +---- 192.168.101.2 -+---- device id 3
              |                                  |
              |                                  +---- device id 4
              |                                  |
              |                                  +---- device id 5
              |
              +---- zone 2 -+---- 192.168.102.1 -+---- device id 6
                            |                    |
                            |                    +---- device id 7
                            |                    |
                            |                    +---- device id 8
                            |
                            +---- 192.168.102.2 -+---- device id 9
                                                 |
                                                 +---- device id 10


    region 2 -+---- zone 1 -+---- 192.168.201.1 -+---- device id 12
                            |                    |
                            |                    +---- device id 13
                            |                    |
                            |                    +---- device id 14
                            |
                            +---- 192.168.201.2 -+---- device id 15
                                                 |
                                                 +---- device id 16
                                                 |
                                                 +---- device id 17

    The tier tree would look like:
    {
      (): [(1,), (2,)],

      (1,): [(1, 1), (1, 2)],
      (2,): [(2, 1)],

      (1, 1): [(1, 1, 192.168.101.1),
               (1, 1, 192.168.101.2)],
      (1, 2): [(1, 2, 192.168.102.1),
               (1, 2, 192.168.102.2)],
      (2, 1): [(2, 1, 192.168.201.1),
               (2, 1, 192.168.201.2)],

      (1, 1, 192.168.101.1): [(1, 1, 192.168.101.1, 0),
                              (1, 1, 192.168.101.1, 1),
                              (1, 1, 192.168.101.1, 2)],
      (1, 1, 192.168.101.2): [(1, 1, 192.168.101.2, 3),
                              (1, 1, 192.168.101.2, 4),
                              (1, 1, 192.168.101.2, 5)],
      (1, 2, 192.168.102.1): [(1, 2, 192.168.102.1, 6),
                              (1, 2, 192.168.102.1, 7),
                              (1, 2, 192.168.102.1, 8)],
      (1, 2, 192.168.102.2): [(1, 2, 192.168.102.2, 9),
                              (1, 2, 192.168.102.2, 10)],
      (2, 1, 192.168.201.1): [(2, 1, 192.168.201.1, 12),
                              (2, 1, 192.168.201.1, 13),
                              (2, 1, 192.168.201.1, 14)],
      (2, 1, 192.168.201.2): [(2, 1, 192.168.201.2, 15),
                              (2, 1, 192.168.201.2, 16),
                              (2, 1, 192.168.201.2, 17)],
    }

    :devices: device dicts from which to generate the tree
    :returns: tier tree

    """
    tier2children = defaultdict(set)
    for dev in devices:
        for tier in tiers_for_dev(dev):
            if len(tier) > 1:
                tier2children[tier[0:-1]].add(tier)
            else:
                tier2children[()].add(tier)
    return tier2children


# we can't store None's in the replica2part2dev array, so we high-jack
# the max value for magic to represent the part is not currently
# assigned to any device.
NONE_DEV = 2 ** 16 - 1
MAX_BALANCE = 999.99


class RingValidationWarning(Warning):
    pass


class RingValidationError(Exception):
    pass


class EmptyRingError(Exception):
    pass


class UnPicklingError(Exception):
    pass

try:
    # python 2.7+
    from logging import NullHandler
except ImportError:
    # python 2.6
    class NullHandler(logging.Handler):
        def emit(self, *a, **kw):
            pass


class RingBuilder(object):
    """
    Used to build swift.common.ring.RingData instances to be written to disk
    and used with swift.common.ring.Ring instances. See bin/swift-ring-builder
    for example usage.

    The instance variable devs_changed indicates if the device information has
    changed since the last balancing. This can be used by tools to know whether
    a rebalance request is an isolated request or due to added, changed, or
    removed devices.

    :param part_power: number of partitions = 2**part_power.
    :param replicas: number of replicas for each partition
    :param min_part_hours: minimum number of hours between partition changes
    """

    def __init__(self, part_power, replicas, min_part_hours):
        if part_power > 32:
            raise ValueError("part_power must be at most 32 (was %d)"
                             % (part_power,))
        if replicas < 1:
            raise ValueError("replicas must be at least 1 (was %.6f)"
                             % (replicas,))
        if min_part_hours < 0:
            raise ValueError("min_part_hours must be non-negative (was %d)"
                             % (min_part_hours,))

        self.part_power = part_power
        self.replicas = replicas
        self.min_part_hours = min_part_hours
        self.parts = 2 ** self.part_power
        self.devs = []
        self.devs_changed = False
        self.version = 0
        self.overload = 0.0

        # _replica2part2dev maps from replica number to partition number to
        # device id. So, for a three replica, 2**23 ring, it's an array of
        # three 2**23 arrays of device ids (unsigned shorts). This can work a
        # bit faster than the 2**23 array of triplet arrays of device ids in
        # many circumstances. Making one big 2**23 * 3 array didn't seem to
        # have any speed change; though you're welcome to try it again (it was
        # a while ago, code-wise, when I last tried it).
        self._replica2part2dev = None

        # _last_part_moves is an array of unsigned bytes representing
        # the number of hours since a given partition was last moved.
        # This is used to guarantee we don't move a partition twice
        # within a given number of hours (24 is my usual test). Removing
        # a device overrides this behavior as it's assumed that's only
        # done because of device failure.
        self._last_part_moves = None
        # _last_part_moves_epoch indicates the time the offsets in
        # _last_part_moves is based on.
        self._last_part_moves_epoch = 0

        self._last_part_gather_start = 0

        self._dispersion_graph = {}
        self.dispersion = 0.0
        self._remove_devs = []
        self._ring = None

        self.logger = logging.getLogger("swift.ring.builder")
        if not self.logger.handlers:
            self.logger.disabled = True
            # silence "no handler for X" error messages
            self.logger.addHandler(NullHandler())

    @contextmanager
    def debug(self):
        """
        Temporarily enables debug logging, useful in tests, e.g.

            with rb.debug():
                rb.rebalance()
        """
        self.logger.disabled = False
        try:
            yield
        finally:
            self.logger.disabled = True

    @property
    def min_part_seconds_left(self):
        """Get the total seconds until a rebalance can be performed"""
        elapsed_seconds = int(time() - self._last_part_moves_epoch)
        return max((self.min_part_hours * 3600) - elapsed_seconds, 0)

    def weight_of_one_part(self):
        """
        Returns the weight of each partition as calculated from the
        total weight of all the devices.
        """
        try:
            return self.parts * self.replicas / \
                sum(d['weight'] for d in self._iter_devs())
        except ZeroDivisionError:
            raise EmptyRingError('There are no devices in this '
                                 'ring, or all devices have been '
                                 'deleted')

    @classmethod
    def from_dict(cls, builder_data):
        b = cls(1, 1, 1)  # Dummy values
        b.copy_from(builder_data)
        return b

    def copy_from(self, builder):
        """
        Reinitializes this RingBuilder instance from data obtained from the
        builder dict given. Code example::

            b = RingBuilder(1, 1, 1)  # Dummy values
            b.copy_from(builder)

        This is to restore a RingBuilder that has had its b.to_dict()
        previously saved.
        """
        if hasattr(builder, 'devs'):
            self.part_power = builder.part_power
            self.replicas = builder.replicas
            self.min_part_hours = builder.min_part_hours
            self.parts = builder.parts
            self.devs = builder.devs
            self.devs_changed = builder.devs_changed
            self.overload = builder.overload
            self.version = builder.version
            self._replica2part2dev = builder._replica2part2dev
            self._last_part_moves_epoch = builder._last_part_moves_epoch
            self._last_part_moves = builder._last_part_moves
            self._last_part_gather_start = builder._last_part_gather_start
            self._remove_devs = builder._remove_devs
        else:
            self.part_power = builder['part_power']
            self.replicas = builder['replicas']
            self.min_part_hours = builder['min_part_hours']
            self.parts = builder['parts']
            self.devs = builder['devs']
            self.devs_changed = builder['devs_changed']
            self.overload = builder.get('overload', 0.0)
            self.version = builder['version']
            self._replica2part2dev = builder['_replica2part2dev']
            self._last_part_moves_epoch = builder['_last_part_moves_epoch']
            self._last_part_moves = builder['_last_part_moves']
            self._last_part_gather_start = builder['_last_part_gather_start']
            self._dispersion_graph = builder.get('_dispersion_graph', {})
            self.dispersion = builder.get('dispersion')
            self._remove_devs = builder['_remove_devs']
        self._ring = None

        # Old builders may not have a region defined for their devices, in
        # which case we default it to 1.
        for dev in self._iter_devs():
            dev.setdefault("region", 1)

        if not self._last_part_moves_epoch:
            self._last_part_moves_epoch = 0

    def __deepcopy__(self, memo):
        return type(self).from_dict(deepcopy(self.to_dict(), memo))

    def to_dict(self):
        """
        Returns a dict that can be used later with copy_from to
        restore a RingBuilder. swift-ring-builder uses this to
        pickle.dump the dict to a file and later load that dict into
        copy_from.
        """
        return {'part_power': self.part_power,
                'replicas': self.replicas,
                'min_part_hours': self.min_part_hours,
                'parts': self.parts,
                'devs': self.devs,
                'devs_changed': self.devs_changed,
                'version': self.version,
                'overload': self.overload,
                '_replica2part2dev': self._replica2part2dev,
                '_last_part_moves_epoch': self._last_part_moves_epoch,
                '_last_part_moves': self._last_part_moves,
                '_last_part_gather_start': self._last_part_gather_start,
                '_dispersion_graph': self._dispersion_graph,
                'dispersion': self.dispersion,
                '_remove_devs': self._remove_devs}

    def _build_dispersion_graph(self, old_replica2part2dev=None):
        """
        Build a dict of all tiers in the cluster to a list of the number of
        parts with a replica count at each index.  The values of the dict will
        be lists of length the maximum whole replica + 1 so that the
        graph[tier][3] is the number of parts within the tier with 3 replicas
        and graph [tier][0] is the number of parts not assigned in this tier.

        i.e.
        {
            <tier>: [
                <number_of_parts_with_0_replicas>,
                <number_of_parts_with_1_replicas>,
                ...
                <number_of_parts_with_n_replicas>,
                ],
            ...
        }

        :param old_replica2part2dev: if called from rebalance, the
            old_replica2part2dev can be used to count moved parts.

        :returns: number of parts with different assignments than
            old_replica2part2dev if provided
        """

        # Since we're going to loop over every replica of every part we'll
        # also count up changed_parts if old_replica2part2dev is passed in
        old_replica2part2dev = old_replica2part2dev or []
        # Compare the partition allocation before and after the rebalance
        # Only changed device ids are taken into account; devices might be
        # "touched" during the rebalance, but actually not really moved
        changed_parts = 0

        int_replicas = int(math.ceil(self.replicas))
        max_allowed_replicas = self._build_max_replicas_by_tier()
        parts_at_risk = 0

        dispersion_graph = {}
        # go over all the devices holding each replica part by part
        for part_id, dev_ids in enumerate(
                zip(*self._replica2part2dev)):
            # count the number of replicas of this part for each tier of each
            # device, some devices may have overlapping tiers!
            replicas_at_tier = defaultdict(int)
            for rep_id, dev in enumerate(iter(
                    self.devs[dev_id] for dev_id in dev_ids)):
                for tier in (dev.get('tiers') or tiers_for_dev(dev)):
                    replicas_at_tier[tier] += 1
                # IndexErrors will be raised if the replicas are increased or
                # decreased, and that actually means the partition has changed
                try:
                    old_device = old_replica2part2dev[rep_id][part_id]
                except IndexError:
                    changed_parts += 1
                    continue

                if old_device != dev['id']:
                    changed_parts += 1
            part_at_risk = False
            # update running totals for each tiers' number of parts with a
            # given replica count
            for tier, replicas in replicas_at_tier.items():
                if tier not in dispersion_graph:
                    dispersion_graph[tier] = [self.parts] + [0] * int_replicas
                dispersion_graph[tier][0] -= 1
                dispersion_graph[tier][replicas] += 1
                if replicas > max_allowed_replicas[tier]:
                    part_at_risk = True
            # this part may be at risk in multiple tiers, but we only count it
            # as at_risk once
            if part_at_risk:
                parts_at_risk += 1
        self._dispersion_graph = dispersion_graph
        self.dispersion = 100.0 * parts_at_risk / self.parts
        return changed_parts

    def _build_balance_per_dev(self):
        """
        Build a map of <device_id> => <balance> where <balance> is a float
        representing the percentage difference from the desired amount of
        partitions a given device wants and the amount it has.

        N.B. this method only considers a device's weight and the parts
        assigned, not the parts wanted according to the replica plan.
        """
        weight_of_one_part = self.weight_of_one_part()
        balance_per_dev = {}
        for dev in self._iter_devs():
            if not dev['weight']:
                if dev['parts']:
                    # If a device has no weight, but has partitions, then its
                    # overage is considered "infinity" and therefore always the
                    # worst possible. We show MAX_BALANCE for convenience.
                    balance = MAX_BALANCE
                else:
                    balance = 0
            else:
                balance = 100.0 * dev['parts'] / (
                    dev['weight'] * weight_of_one_part) - 100.0
            balance_per_dev[dev['id']] = balance
        return balance_per_dev

    def get_balance(self):
        """
        Get the balance of the ring. The balance value is the highest
        percentage of the desired amount of partitions a given device
        wants. For instance, if the "worst" device wants (based on its
        weight relative to the sum of all the devices' weights) 123
        partitions and it has 124 partitions, the balance value would
        be 0.83 (1 extra / 123 wanted * 100 for percentage).

        :returns: balance of the ring
        """
        balance_per_dev = self._build_balance_per_dev()
        return max(abs(b) for b in balance_per_dev.values())

    def get_required_overload(self, weighted=None, wanted=None):
        """
        Returns the minimum overload value required to make the ring maximally
        dispersed.

        The required overload is the largest percentage change of any single
        device from its weighted replicanth to its wanted replicanth (note:
        under weighted devices have a negative percentage change) to archive
        dispersion - that is to say a single device that must be overloaded by
        5% is worse than 5 devices in a single tier overloaded by 1%.
        """
        weighted = weighted or self._build_weighted_replicas_by_tier()
        wanted = wanted or self._build_wanted_replicas_by_tier()
        max_overload = 0.0
        for dev in self._iter_devs():
            tier = (dev['region'], dev['zone'], dev['ip'], dev['id'])
            if not dev['weight']:
                if tier not in wanted or not wanted[tier]:
                    continue
                raise RingValidationError(
                    'Device %s has zero weight and '
                    'should not want any replicas' % (tier,))
            required = (wanted[tier] - weighted[tier]) / weighted[tier]
            self.logger.debug('%s wants %s and is weighted for %s so '
                              'therefore requires %s overload' % (
                                  tier, wanted[tier], weighted[tier],
                                  required))
            if required > max_overload:
                max_overload = required
        return max_overload

    def pretend_min_part_hours_passed(self):
        """
        Override min_part_hours by marking all partitions as having been moved
        255 hours ago and last move epoch to 'the beginning of time'. This can
        be used to force a full rebalance on the next call to rebalance.
        """
        self._last_part_moves_epoch = 0
        if not self._last_part_moves:
            return
        for part in range(self.parts):
            self._last_part_moves[part] = 0xff

    def get_part_devices(self, part):
        """
        Get the devices that are responsible for the partition,
        filtering out duplicates.

        :param part: partition to get devices for
        :returns: list of device dicts
        """
        devices = []
        for dev in self._devs_for_part(part):
            if dev not in devices:
                devices.append(dev)
        return devices

    def _iter_devs(self):
        """
        Returns an iterator all the non-None devices in the ring. Note that
        this means list(b._iter_devs())[some_id] may not equal b.devs[some_id];
        you will have to check the 'id' key of each device to obtain its
        dev_id.
        """
        for dev in self.devs:
            if dev is not None:
                yield dev

    def _build_tier2children(self):
        """
        Wrap helper build_tier_tree so exclude zero-weight devices.
        """
        return build_tier_tree(d for d in self._iter_devs() if d['weight'])

    @staticmethod
    def _sort_key_for(dev):
        return (dev['parts_wanted'], random.randint(0, 0xFFFF), dev['id'])

    def _build_max_replicas_by_tier(self, bound=math.ceil):
        """
        Returns a defaultdict of (tier: replica_count) for all tiers in the
        ring excluding zero weight devices.

        There will always be a () entry as the root of the structure, whose
        replica_count will equal the ring's replica_count.

        Then there will be (region,) entries for each region, indicating the
        maximum number of replicas the region might have for any given
        partition.

        Next there will be (region, zone) entries for each zone, indicating
        the maximum number of replicas in a given region and zone.  Anything
        greater than 1 indicates a partition at slightly elevated risk, as if
        that zone were to fail multiple replicas of that partition would be
        unreachable.

        Next there will be (region, zone, ip_port) entries for each node,
        indicating the maximum number of replicas stored on a node in a given
        region and zone.  Anything greater than 1 indicates a partition at
        elevated risk, as if that ip_port were to fail multiple replicas of
        that partition would be unreachable.

        Last there will be (region, zone, ip_port, device) entries for each
        device, indicating the maximum number of replicas the device shares
        with other devices on the same node for any given partition.
        Anything greater than 1 indicates a partition at serious risk, as the
        data on that partition will not be stored distinctly at the ring's
        replica_count.

        Example return dict for the common SAIO setup::

            {(): 3.0,
            (1,): 3.0,
            (1, 1): 1.0,
            (1, 1, '127.0.0.1:6010'): 1.0,
            (1, 1, '127.0.0.1:6010', 0): 1.0,
            (1, 2): 1.0,
            (1, 2, '127.0.0.1:6020'): 1.0,
            (1, 2, '127.0.0.1:6020', 1): 1.0,
            (1, 3): 1.0,
            (1, 3, '127.0.0.1:6030'): 1.0,
            (1, 3, '127.0.0.1:6030', 2): 1.0,
            (1, 4): 1.0,
            (1, 4, '127.0.0.1:6040'): 1.0,
            (1, 4, '127.0.0.1:6040', 3): 1.0}

        """
        # Used by walk_tree to know what entries to create for each recursive
        # call.
        tier2children = self._build_tier2children()

        def walk_tree(tier, replica_count):
            if len(tier) == 4:
                # special case for device, it's not recursive
                replica_count = min(1, replica_count)
            mr = {tier: replica_count}
            if tier in tier2children:
                subtiers = tier2children[tier]
                for subtier in subtiers:
                    submax = bound(float(replica_count) / len(subtiers))
                    mr.update(walk_tree(subtier, submax))
            return mr
        mr = defaultdict(float)
        mr.update(walk_tree((), self.replicas))
        return mr

    def _build_weighted_replicas_by_tier(self):
        """
        Returns a dict mapping <tier> => replicanths for all tiers in
        the ring based on their weights.
        """
        weight_of_one_part = self.weight_of_one_part()

        # assign each device some replicanths by weight (can't be > 1)
        weighted_replicas_for_dev = {}
        devices_with_room = []
        for dev in self._iter_devs():
            if not dev['weight']:
                continue
            weighted_replicas = (
                dev['weight'] * weight_of_one_part / self.parts)
            if weighted_replicas < 1:
                devices_with_room.append(dev['id'])
            else:
                weighted_replicas = 1
            weighted_replicas_for_dev[dev['id']] = weighted_replicas

        while True:
            remaining = self.replicas - sum(weighted_replicas_for_dev.values())
            if remaining < 1e-10:
                break
            devices_with_room = [d for d in devices_with_room if
                                 weighted_replicas_for_dev[d] < 1]
            rel_weight = remaining / sum(
                weighted_replicas_for_dev[d] for d in devices_with_room)
            for d in devices_with_room:
                weighted_replicas_for_dev[d] = min(
                    1, weighted_replicas_for_dev[d] * (rel_weight + 1))

        weighted_replicas_by_tier = defaultdict(float)
        for dev in self._iter_devs():
            if not dev['weight']:
                continue
            assigned_replicanths = weighted_replicas_for_dev[dev['id']]
            dev_tier = (dev['region'], dev['zone'], dev['ip'], dev['id'])
            for i in range(len(dev_tier) + 1):
                tier = dev_tier[:i]
                weighted_replicas_by_tier[tier] += assigned_replicanths

        # belts & suspenders/paranoia -  at every level, the sum of
        # weighted_replicas should be very close to the total number of
        # replicas for the ring
        tiers = ['cluster', 'regions', 'zones', 'servers', 'devices']
        for i, tier_name in enumerate(tiers):
            replicas_at_tier = sum(weighted_replicas_by_tier[t] for t in
                                   weighted_replicas_by_tier if len(t) == i)
            if abs(self.replicas - replicas_at_tier) > 1e-10:
                raise RingValidationError(
                    '%s != %s at tier %s' % (
                        replicas_at_tier, self.replicas, tier_name))

        return weighted_replicas_by_tier

    def _build_wanted_replicas_by_tier(self):
        """
        Returns a defaultdict of (tier: replicanths) for all tiers in the ring
        based on unique-as-possible (full dispersion) with respect to their
        weights and device counts.

        N.B.  _build_max_replicas_by_tier calculates the upper bound on the
        replicanths each tier may hold irrespective of the weights of the
        tier; this method will calculate the minimum replicanth <=
        max_replicas[tier] that will still solve dispersion.  However, it is
        not guaranteed to return a fully dispersed solution if failure domains
        are over-weighted for their device count.
        """
        weighted_replicas = self._build_weighted_replicas_by_tier()
        dispersed_replicas = {
            t: {
                'min': math.floor(r),
                'max': math.ceil(r),
            } for (t, r) in
            self._build_max_replicas_by_tier(bound=float).items()
        }

        # watch out for device limited tiers
        num_devices = defaultdict(int)
        for d in self._iter_devs():
            if d['weight'] <= 0:
                continue
            for t in (d.get('tiers') or tiers_for_dev(d)):
                num_devices[t] += 1
            num_devices[()] += 1

        tier2children = self._build_tier2children()

        wanted_replicas = defaultdict(float)

        def place_replicas(tier, replicanths):
            if replicanths > num_devices[tier]:
                raise RingValidationError(
                    'More replicanths (%s) than devices (%s) '
                    'in tier (%s)' % (replicanths, num_devices[tier], tier))
            wanted_replicas[tier] = replicanths
            sub_tiers = sorted(tier2children[tier])
            if not sub_tiers:
                return

            to_place = defaultdict(float)
            remaining = replicanths
            tiers_to_spread = sub_tiers
            device_limited = False

            while True:
                rel_weight = remaining / sum(weighted_replicas[t]
                                             for t in tiers_to_spread)
                for t in tiers_to_spread:
                    replicas = to_place[t] + (
                        weighted_replicas[t] * rel_weight)
                    if replicas < dispersed_replicas[t]['min']:
                        replicas = dispersed_replicas[t]['min']
                    elif (replicas > dispersed_replicas[t]['max'] and
                          not device_limited):
                        replicas = dispersed_replicas[t]['max']
                    if replicas > num_devices[t]:
                        replicas = num_devices[t]
                    to_place[t] = replicas

                remaining = replicanths - sum(to_place.values())

                if remaining < -1e-10:
                    tiers_to_spread = [
                        t for t in sub_tiers
                        if to_place[t] > dispersed_replicas[t]['min']
                    ]
                elif remaining > 1e-10:
                    tiers_to_spread = [
                        t for t in sub_tiers
                        if (num_devices[t] > to_place[t] <
                            dispersed_replicas[t]['max'])
                    ]
                    if not tiers_to_spread:
                        device_limited = True
                        tiers_to_spread = [
                            t for t in sub_tiers
                            if to_place[t] < num_devices[t]
                        ]
                else:
                    # remaining is "empty"
                    break

            for t in sub_tiers:
                self.logger.debug('Planning %s on %s',
                                  to_place[t], t)
                place_replicas(t, to_place[t])

        # place all replicas in the cluster tier
        place_replicas((), self.replicas)

        # belts & suspenders/paranoia -  at every level, the sum of
        # wanted_replicas should be very close to the total number of
        # replicas for the ring
        tiers = ['cluster', 'regions', 'zones', 'servers', 'devices']
        for i, tier_name in enumerate(tiers):
            replicas_at_tier = sum(wanted_replicas[t] for t in
                                   wanted_replicas if len(t) == i)
            if abs(self.replicas - replicas_at_tier) > 1e-10:
                raise RingValidationError(
                    '%s != %s at tier %s' % (
                        replicas_at_tier, self.replicas, tier_name))

        return wanted_replicas

    def _build_target_replicas_by_tier(self):
        """
        Build a map of <tier> => <target_replicas> accounting for device
        weights, unique-as-possible dispersion and overload.

        <tier> - a tuple, describing each tier in the ring topology
        <target_replicas> - a float, the target replicanths at the tier
        """
        weighted_replicas = self._build_weighted_replicas_by_tier()
        wanted_replicas = self._build_wanted_replicas_by_tier()
        max_overload = self.get_required_overload(weighted=weighted_replicas,
                                                  wanted=wanted_replicas)
        if max_overload <= 0.0:
            return wanted_replicas
        else:
            overload = min(self.overload, max_overload)
        self.logger.debug("Using effective overload of %f", overload)
        target_replicas = defaultdict(float)
        for tier, weighted in weighted_replicas.items():
            m = (wanted_replicas[tier] - weighted) / max_overload
            target_replicas[tier] = m * overload + weighted

        # belts & suspenders/paranoia -  at every level, the sum of
        # target_replicas should be very close to the total number
        # of replicas for the ring
        tiers = ['cluster', 'regions', 'zones', 'servers', 'devices']
        for i, tier_name in enumerate(tiers):
            replicas_at_tier = sum(target_replicas[t] for t in
                                   target_replicas if len(t) == i)
            if abs(self.replicas - replicas_at_tier) > 1e-10:
                raise RingValidationError(
                    '%s != %s at tier %s' % (
                        replicas_at_tier, self.replicas, tier_name))

        return target_replicas

    def _build_replica_plan(self):
        """
        Wraps return value of _build_target_replicas_by_tier to include
        pre-calculated min and max values for each tier.

        :returns: a dict, mapping <tier> => <replica_plan>, where
                  <replica_plan> is itself a dict

        <replica_plan> include at least the following keys:

            min - the minimum number of replicas at the tier
            target - the target replicanths at the tier
            max - the maximum number of replicas at the tier
        """
        # replica part-y planner!
        target_replicas = self._build_target_replicas_by_tier()
        replica_plan = defaultdict(
            lambda: {'min': 0, 'target': 0, 'max': 0})
        replica_plan.update({
            t: {
                'min': math.floor(r + 1e-10),
                'target': r,
                'max': math.ceil(r - 1e-10),
            } for (t, r) in
            target_replicas.items()
        })
        return replica_plan

    def _devs_for_part(self, part):
        """
        Returns a list of devices for a specified partition.

        Deliberately includes duplicates.
        """
        if self._replica2part2dev is None:
            return []
        devs = []
        for part2dev in self._replica2part2dev:
            if part >= len(part2dev):
                continue
            dev_id = part2dev[part]
            if dev_id == NONE_DEV:
                continue
            devs.append(self.devs[dev_id])
        return devs

    def _replicas_for_part(self, part):
        """
        Returns a list of replicas for a specified partition.

        These can be used as indices into self._replica2part2dev
        without worrying about IndexErrors.
        """
        return [replica for replica, part2dev
                in enumerate(self._replica2part2dev)
                if part < len(part2dev)]

    def _each_part_replica(self):
        """
        Generator yielding every (partition, replica) pair in the ring.
        """
        for replica, part2dev in enumerate(self._replica2part2dev):
            for part in range(len(part2dev)):
                yield (part, replica)

    @classmethod
    def load(cls, builder_readable):
        """
        Obtain RingBuilder instance of the provided builder file obj

        :param builder_readable: open file like thing
        :return: RingBuilder instance
        """
        try:
            builder = pickle.load(builder_readable)
        except Exception:
            # raise error during unpickling as UnPicklingError
            raise UnPicklingError('Ring Builder file is invalid!')

        if not hasattr(builder, 'devs'):
            builder_dict = builder
            builder = RingBuilder(1, 1, 1)
            builder.copy_from(builder_dict)
        for dev in builder.devs:
            # really old rings didn't have meta keys
            if dev and 'meta' not in dev:
                dev['meta'] = ''
            # NOTE(akscram): An old ring builder file don't contain
            #                replication parameters.
            if dev:
                if 'ip' in dev:
                    dev.setdefault('replication_ip', dev['ip'])
                if 'port' in dev:
                    dev.setdefault('replication_port', dev['port'])
        return builder
