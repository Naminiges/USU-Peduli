(function () {

  function openLightbox(images, index) {
    var i = index || 0;

    var overlay = document.createElement('div');
    overlay.className = 'simple-lightbox';

    var img = document.createElement('img');
    img.src = images[i];

    var prev = document.createElement('div');
    prev.className = 'lb-btn lb-prev';
    prev.innerHTML = '‹';

    var next = document.createElement('div');
    next.className = 'lb-btn lb-next';
    next.innerHTML = '›';

    var close = document.createElement('div');
    close.className = 'lb-btn lb-close';
    close.innerHTML = '✕';

    function update() {
      img.src = images[i];
    }

    prev.onclick = function (e) {
      e.stopPropagation();
      i = (i - 1 + images.length) % images.length;
      update();
    };

    next.onclick = function (e) {
      e.stopPropagation();
      i = (i + 1) % images.length;
      update();
    };

    close.onclick = function () {
      document.body.removeChild(overlay);
    };

    overlay.onclick = function (e) {
      if (e.target === overlay) close.onclick();
    };

    overlay.appendChild(img);
    overlay.appendChild(prev);
    overlay.appendChild(next);
    overlay.appendChild(close);
    document.body.appendChild(overlay);
  }

  window.initSimpleGallery = function (root) {
    if (!root || root._galleryInited) return;
    root._galleryInited = true;

    var data = root.getAttribute('data-images');
    if (!data) return;

    var images;
    try {
      images = JSON.parse(data);
    } catch (e) {
      console.warn('Invalid gallery data', e);
      return;
    }

    if (!images.length) return;

    root.onclick = function () {
      openLightbox(images, 0);
    };
  };

  // 🔥 AUTO INIT UNTUK SEMUA YANG SUDAH ADA DI DOM
  window.initAllGalleries = function (scope) {
    (scope || document)
      .querySelectorAll('.simple-gallery')
      .forEach(function (el) {
        initSimpleGallery(el);
      });
  };

})();
