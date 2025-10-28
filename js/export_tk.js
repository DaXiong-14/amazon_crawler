
function sellersprite_token(e, t, n, r) {
    let a = [],
    s = [e, t, n, r];
    for (let e = 0; e < s.length; e++)
    if (s[e] && null != s[e] && s[e].toString().length > 0) {
        let t = a.length;
        for (let t = 0; s[e] instanceof Array && t < s[e].length; t++)
        a.push(s[e][t]);
        t === a.length && a.push(s[e].toString());
    }
    return a.length < 1 ? "" : _cal(a.join(""), this.EXT_VERSION);
}

function _cal(e, t) {
    function n(e, t) {
    for (let n = 0; n < t.length - 2; n += 3) {
        let r = t.charAt(n + 2);
        (r = "a" <= r ? r.charCodeAt(0) - 87 : Number(r)),
        (r = "+" == t.charAt(n + 1) ? e >>> r : e << r),
        (e = "+" == t.charAt(n) ? (e + r) & 4294967295 : e ^ r);
    }
    return e;
    }
    function r(e, t) {
    var r = '400801.1364508470'.split(".");
    t = Number(r[0]) || 0;
    for (var a = [], s = 0, i = 0; i < e.length; i++) {
        var o = e.charCodeAt(i);
        128 > o
        ? (a[s++] = o)
        : (2048 > o
            ? (a[s++] = (o >> 6) | 192)
            : (55296 == (64512 & o) &&
                i + 1 < e.length &&
                56320 == (64512 & e.charCodeAt(i + 1))
                ? ((o =
                    65536 +
                    ((1023 & o) << 10) +
                    (1023 & e.charCodeAt(++i))),
                    (a[s++] = (o >> 18) | 240),
                    (a[s++] = ((o >> 12) & 63) | 128))
                : (a[s++] = (o >> 12) | 224),
                (a[s++] = ((o >> 6) & 63) | 128)),
            (a[s++] = (63 & o) | 128));
    }
    for (e = t, s = 0; s < a.length; s++) e = n((e += a[s]), "+-a^+6");
    return (
        (e = n(e, "+-3^+b+-f")),
        0 > (e ^= Number(r[1]) || 0) &&
        (e = 2147483648 + (2147483647 & e)),
        (r = e % 1e6).toString() + "." + (r ^ t)
    );
    }
    return r(e, t);
}
