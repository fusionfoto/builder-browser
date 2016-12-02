$(function () {
  $('div.tier').hover(function() {
    var hovered = this
    $('div.tier').each(function() {
      var target = "10%";
      if (this == hovered) {
        target = "70%"
      }
      $(this).animate({ width: target });
    });
  });
  $('div.regions ul').show();
  $('.ring li').click(function() {
    var selected_tier_item = $(this);
    var target_list = $("#" + selected_tier_item.data("target"));
    if ( target_list.length === 0 ) {
      return;
    }
    var all_tiers = $('div.tier');
    // hide selected and child tiers arrows
    var selected_item_tier = selected_tier_item.parents('div.tier');
    var selected_tier_index = all_tiers.index(selected_item_tier);
    var selected_and_child_tiers = all_tiers.slice(selected_tier_index);
    selected_and_child_tiers.find('div.arrow').hide();
    // hide child tier lists
    var child_tiers = all_tiers.slice(
        all_tiers.index(
          target_list.parents('div.tier')));
    child_tiers.children('ul').hide();
    child_tiers.children('div.buffer').height(0);
    // show the selected_tier_item arrow
    selected_tier_item.children('div.arrow').fadeIn();
    // show the target
    target_list.fadeIn();
    // push the target down to align
    var selected_top = selected_tier_item.position().top;
    var target_list_last_top = target_list.children('li').last().position().top;
    target_list.prev('div.buffer').height(
        Math.max(0, selected_top - target_list_last_top));
  });
  $('[data-width]').each(function () {
    var $this = $(this);
    var width = $this.data('width');
    if ( width ) {
      $this.css('width', width + '%');
    } else {
      $this.css('width', 0);
    }
  });
});
