import itertools
from flask import Flask, request, render_template
import random

import ring


app = Flask(__name__)


def dispersion_report(builder):
    builder._build_dispersion_graph()
    max_allowed_replicas = builder._build_max_replicas_by_tier()
    report = {}
    for tier, replica_counts in builder._dispersion_graph.items():
        assigned = over = 0
        max_replicas = int(max_allowed_replicas[tier])
        for replica_count, num_parts in enumerate(replica_counts):
            assigned += replica_count * num_parts
            if replica_count > max_replicas:
                delta_count = (replica_count - max_replicas) * num_parts
                over += delta_count
                assigned -= delta_count
        report[tier] = assigned, over
    return report


def inspect_builder(builder):
    data = {}
    for ring_device in builder._iter_devs():
        if ring_device['parts'] == 0:
            continue
        regions = data.setdefault('regions', [])
        for region in regions:
            if region['id'] == ring_device['region']:
                break
        else:
            region = {
                'id': ring_device['region'],
                'name': 'Region %s' % ring_device['region'],
                'zones': [],
                'wanted_parts': 0,
                'assigned_parts': 0,
                'total_parts': 0,
                'over_parts': 0,
            }
            data['regions'].append(region)
        for zone in region['zones']:
            if zone['id'] == ring_device['zone']:
                break
        else:
            zone = {
                'id': ring_device['zone'],
                'name': 'Zone %s' % ring_device['zone'],
                'nodes': [],
                'wanted_parts': 0,
                'assigned_parts': 0,
                'total_parts': 0,
                'over_parts': 0,
            }
            region['zones'].append(zone)
        node_id = (
            ring_device['region'], ring_device['zone'], ring_device['ip'])
        node_id = 'r%sz%s-%s' % (ring_device['region'], ring_device['zone'],
                                 ring_device['ip'].replace('.', '-'))
        for node in zone['nodes']:
            if node['id'] == node_id:
                break
        else:
            node = {
                'id': node_id,
                'ip': ring_device['ip'],
                'name': ring_device['ip'],
                'devs': [],
                'wanted_parts': 0,
                'assigned_parts': 0,
                'total_parts': 0,
                'over_parts': 0,
            }
            zone['nodes'].append(node)
        dev = {
            'id': ring_device['id'],
            'name': ring_device['device'],
            'wanted_parts': 0,
            'assigned_parts': 0,
            'total_parts': 0,
            'over_parts': 0,
        }
        node['devs'].append(dev)

    max_parts_at_tier = {
        'regions': 0,
        'zones': 0,
        'nodes': 0,
        'devs': 0,
    }
    weight_of_one_part = builder.weight_of_one_part()
    report = dispersion_report(builder)
    for region in data['regions']:
        tier = (region['id'],)
        region['assigned_parts'], region['over_parts'] = report[tier]
        for zone in region['zones']:
            tier = (region['id'], zone['id'])
            zone['assigned_parts'], zone['over_parts'] = report[tier]
            for node in zone['nodes']:
                tier = (region['id'], zone['id'], node['ip'])
                node['assigned_parts'], node['over_parts'] = report[tier]
                for dev in node['devs']:
                    tier = (region['id'], zone['id'], node['ip'], dev['id'])
                    dev['assigned_parts'], dev['over_parts'] = report[tier]
                    ring_device = builder.devs[dev['id']]
                    wanted_parts = ring_device['weight'] * weight_of_one_part
                    for item in (region, zone, node, dev):
                        item['wanted_parts'] += wanted_parts
                        item['total_parts'] += ring_device['parts']
                    max_parts_at_tier['devs'] = max((
                        max_parts_at_tier['devs'],
                        dev['wanted_parts'],
                        dev['total_parts'],
                    ))
                max_parts_at_tier['nodes'] = max((
                    max_parts_at_tier['nodes'],
                    node['wanted_parts'],
                    node['total_parts'],
                ))
            max_parts_at_tier['zones'] = max((
                max_parts_at_tier['zones'],
                zone['wanted_parts'],
                zone['total_parts'],
            ))
        max_parts_at_tier['regions'] = max((
            max_parts_at_tier['regions'],
            region['wanted_parts'],
            region['total_parts'],
        ))
    for tier, max_parts in max_parts_at_tier.items():
        max_parts_at_tier[tier] = 1.0 * max_parts
    for region in data['regions']:
        for zone in region['zones']:
            for node in zone['nodes']:
                for dev in node['devs']:
                    dev['wanted'] = 100 * (
                        dev['wanted_parts'] / max_parts_at_tier['devs'])
                    dev['assigned'] = 100 * (
                        dev['assigned_parts'] / max_parts_at_tier['devs'])
                    dev['over'] = 100 * (
                        dev['over_parts'] / max_parts_at_tier['devs'])
                node['wanted'] = 100 * (
                    node['wanted_parts'] / max_parts_at_tier['nodes'])
                node['assigned'] = 100 * (
                    node['assigned_parts'] / max_parts_at_tier['nodes'])
                node['over'] = 100 * (
                    node['over_parts'] / max_parts_at_tier['nodes'])
            zone['wanted'] = 100 * (
                zone['wanted_parts'] / max_parts_at_tier['zones'])
            zone['assigned'] = 100 * (
                zone['assigned_parts'] / max_parts_at_tier['zones'])
            zone['over'] = 100 * (
                zone['over_parts'] / max_parts_at_tier['zones'])
        region['wanted'] = 100 * (
            region['wanted_parts'] / max_parts_at_tier['regions'])
        region['assigned'] = 100 * (
            region['assigned_parts'] / max_parts_at_tier['regions'])
        region['over'] = 100 * (
            region['over_parts'] / max_parts_at_tier['regions'])
    return data


def random_parts():
    assigned = random.randint(10, 100)
    if random.random() > 0.5:
        over = random.randint(0, 100 - assigned)
    else:
        over = 0
    return {
        'wanted': random.randint(0, 100),
        'assigned': assigned,
        'over': over,
    }


def example_data():
    num_regions = 2
    num_zones = 5
    num_nodes = 8
    num_devices = 100
    data = {
        'regions': []
    }
    region_ids = itertools.count()
    zone_ids = itertools.count()
    node_ids = itertools.count()
    dev_ids = itertools.count()
    for region_ in range(num_regions):
        region_id = next(region_ids)
        region = {
            'id': region_id,
            'name': 'Region %s' % region_id,
            'zones': []
        }
        region.update(random_parts())
        data['regions'].append(region)
    all_zones = []
    for zone_ in range(num_zones):
        zone_id = next(zone_ids)
        zone = {
            'id': zone_id,
            'name': 'Zone %s' % zone_id,
            'nodes': [],
        }
        zone.update(random_parts())
        for region in data['regions']:
            if not region['zones']:
                break
        else:
            region = random.choice(data['regions'])
        region['zones'].append(zone)
        all_zones.append(zone)
    all_nodes = []
    for node_ in range(num_nodes):
        node_id = next(node_ids)
        node = {
            'id': node_id,
            'name': 'Node %s' % node_id,
            'devs': [],
        }
        node.update(random_parts())
        for zone in all_zones:
            if not zone['nodes']:
                break
        else:
            zone = random.choice(all_zones)
        zone['nodes'].append(node)
        all_nodes.append(node)
    for dev_ in range(num_devices):
        dev_id = next(dev_ids)
        dev = {
            'id': dev_id,
            'name': 'Device %s' % dev_id,
        }
        dev.update(random_parts())
        for node in all_nodes:
            if not node['devs']:
                break
        else:
            node = random.choice(all_nodes)
        node['devs'].append(dev)
    return data


@app.route("/", methods=['GET', 'POST'])
def root():
    if request.method == 'POST':
        builder = ring.RingBuilder.load(request.files['file'])
        data = inspect_builder(builder)
    else:
        data = example_data()
    return render_template('index.html', data=data)


if __name__ == "__main__":
    app.run(debug=True)
