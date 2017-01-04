/*!{id:"uupaa.js",ver:0.7,license:"MIT",author:"uupaa.js@gmail.com"}*/
window.sprintf || (function() {
var _BITS = { i: 0x8011, d: 0x8011, u: 0x8021, o: 0x8161, x: 0x8261,
              X: 0x9261, f: 0x92, c: 0x2800, s: 0x84 },
    _PARSE = /%(?:(\d+)\$)?(#|0)?(\d+)?(?:\.(\d+))?(l)?([%iduoxXfcs])/g;

window.sprintf = _sprintf;

function _sprintf(format) {
  function _fmt(m, argidx, flag, width, prec, size, types) {
    if (types === "%") { return "%"; }
    var v = "", w = _BITS[types], overflow, pad;

    idx = argidx ? parseInt(argidx) : next++;

    w & 0x400 || (v = (av[idx] === void 0) ? "" : av[idx]);
    w & 3 && (v = (w & 1) ? parseInt(v) : parseFloat(v), v = isNaN(v) ? "": v);
    w & 4 && (v = ((types === "s" ? v : types) || "").toString());
    w & 0x20  && (v = (v >= 0) ? v : v % 0x100000000 + 0x100000000);
    w & 0x300 && (v = v.toString(w & 0x100 ? 8 : 16));
    w & 0x40  && (flag === "#") && (v = ((w & 0x100) ? "0" : "0x") + v);
    w & 0x80  && prec && (v = (w & 2) ? v.toFixed(prec) : v.slice(0, prec));
    w & 0x6000 && (overflow = (typeof v !== "number" || v < 0));
    w & 0x2000 && (v = overflow ? "" : String.fromCharCode(v));
    w & 0x8000 && (flag = (flag === "0") ? "" : flag);
    v = w & 0x1000 ? v.toString().toUpperCase() : v.toString();

    if (!(w & 0x800 || width === void 0 || v.length >= width)) {
      pad = Array(width - v.length + 1).join(!flag ? " " : flag === "#" ? " " : flag);
      v = ((w & 0x10 && flag === "0") && !v.indexOf("-"))
        ? ("-" + pad + v.slice(1)) : (pad + v);
    }
    return v;
  }
  var next = 1, idx = 0, av = arguments;

  return format.replace(_PARSE, _fmt);
}

})();
