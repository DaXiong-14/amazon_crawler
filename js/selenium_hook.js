(function() {
    if (window._interceptedStylesnapArr) return;
    window._interceptedStylesnapArr = [];
    var origFetch = window.fetch;
    window.fetch = function() {
        var url = arguments[0];
        if (url && url.includes('upload?stylesnapToken')) {
            return origFetch.apply(this, arguments).then(function(resp) {
                resp.clone().text().then(function(txt) {
                    window._interceptedStylesnapArr.push({url: url, body: txt});
                });
                return resp;
            });
        }
        return origFetch.apply(this, arguments);
    };
    var origXHROpen = XMLHttpRequest.prototype.open;
    XMLHttpRequest.prototype.open = function() {
        this._url = arguments[1];
        origXHROpen.apply(this, arguments);
    };
    var origXHRSend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.send = function() {
        this.addEventListener('load', function() {
            if (this._url && this._url.includes('upload?stylesnapToken')) {
                window._interceptedStylesnapArr.push({url: this._url, body: this.responseText});
            }
        });
        origXHRSend.apply(this, arguments);
    };
})();