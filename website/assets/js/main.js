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

  function restoreGraphic() {
    toggleGraphicClasses();
    $("#additive-schema-container").append($('#additive-schema-text'));
    $("#bitemporal-container").append($('#bitemporal-text'))
    $('#additive-schema').remove($('#additive-schema-text'));
    $('#bitemporal').remove($('#bitemporal-text'));
  }


  function shrinkGraphic() {
    toggleGraphicClasses();
    $('#additive-schema').append($('#additive-schema-text'));
    $('#bitemporal').append($('#bitemporal-text'));
  }


  function checkSize() {
    var mediaQueryXs = checkIfBlock('.viewport-check .d-block');
    var mediaQuerySm = checkIfBlock('.viewport-check .d-sm-block');
    var mediaQueryMd = checkIfBlock('.viewport-check .d-md-block');
    var mediaQueryLg = checkIfBlock('.viewport-check .d-lg-block');
    var mediaQueryXl = checkIfBlock('.viewport-check .d-xl-block');


    if (mediaQueryXs) {
      $("body").removeClass().toggleClass("media-query-xs");
      shrinkGraphic();
    }

    if (mediaQuerySm) {
      $("body").removeClass().toggleClass("media-query-sm");
      shrinkGraphic();
    }


    if (mediaQueryMd) {
      $("body").removeClass().toggleClass("media-query-md");
      // Reformat graphic
      shrinkGraphic();
      //$('#additive-schema').append($('#additive-schema-text'));
      //$('#bitemporal').append($('#bitemporal-text'));
    }

    if (mediaQueryLg) {
      $("body").removeClass().toggleClass("media-query-lg");
      // Restore graphic
      restoreGraphic();
    }

    if (mediaQueryXl) {
      $("body").removeClass().toggleClass("media-query-xl");
      restoreGraphic();
    }
  }

  $(window).resize(function() {
    checkSize();
  });

  // Load detection script
  checkSize();


});


$(".feature").mouseover(function() {
  $(this).children().css("color","#E94D4D");
})
.mouseout(function() {
  $(this).find("h4").css("color","#4A4A4A");
  $(this).find("p").css("color","##979797");
});
