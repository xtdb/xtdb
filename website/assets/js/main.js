$(document).ready(function() {
  // Add some invisible elements with Bootstrap CSS visibile utility classes
  $("body").append("<div style='display:none;' class='viewport-check'><span class='d-block'></span><span class='d-sm-block'></span><span class='d-md-block'></span><span class='d-lg-block'></span><span class='d-xl-block'></span></div>");

  // Checks if the span is set to display blcok via CSS
  function checkIfBlock(target) {
    var target = $(target).css('display') == 'block';
    return target;
  }

  function toggleGraphicClasses() {
    $('#additive-schema').toggleClass("bottom");
    $('#bitemporal').toggleClass("bottom");
    $('#additive-schema-text').toggleClass("top");
    $('#bitemporal-text').toggleClass("top");
  }

  function checkSize() {
    var mediaQueryMd = checkIfBlock('.viewport-check .d-md-block');
    var mediaQueryLg = checkIfBlock('.viewport-check .d-lg-block');
    var mediaQueryXl = checkIfBlock('.viewport-check .d-xl-block');

    if (mediaQueryMd == true) {
      $("body").removeClass().toggleClass("media-query-md");
      // Reformat graphic
      toggleGraphicClasses();
      $('#additive-schema').append($('#additive-schema-text'));
      $('#bitemporal').append($('#bitemporal-text'));
    }

    if (mediaQueryLg == true) {
      $("body").removeClass().toggleClass("media-query-lg");
      // Restore graphic
      restoreGraphic();
    }

    if (mediaQueryXl == true) {
      $("body").removeClass().toggleClass("media-query-xl");
      restoreGraphic();
    }
  }

  function restoreGraphic() {
    toggleGraphicClasses();
    $("#additive-schema-container").append($('#additive-schema-text'));
    $("#bitemporal-container").append($('#bitemporal-text'))
    $('#additive-schema').remove($('#additive-schema-text'));
    $('#bitemporal').remove($('#bitemporal-text'));
  }

  $(window).resize(function() {
    checkSize();
  });

  // Load detection script
  checkSize();

});
