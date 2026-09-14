(function () {
  var q = new URLSearchParams(location.search);

  if (q.get("me") === "1") {
    localStorage.setItem("owner", "1");
  }
  if (localStorage.getItem("owner") === "1") return;

  var src = (q.get("s") || "").replace(/[^a-z0-9_-]/gi, "").slice(0, 16);
  if (src) sessionStorage.setItem("src", src);
  else src = sessionStorage.getItem("src") || "";

  var page = location.pathname.split("/").pop() || "index.html";
  var ref  = document.referrer ? new URL(document.referrer).hostname : "direct";
  var os   = (navigator.userAgent.match(/\(([^)]+)\)/) || ["", "?"])[1];
  var tag  = src ? "[" + src + "] " : "";

  fetch("https://ipwho.is/?fields=city,region,country,connection")
    .then(function (r) { return r.json(); })
    .catch(function () { return {}; })
    .then(function (g) {
      var loc = g.city ? g.city + ", " + g.region : (g.country || "??");
      var net = g.connection ? " | " + g.connection.isp : "";
      fetch("https://ntfy.sh/pv7k2mx9qwkneb-visits", {
        method: "POST",
        body: tag + page + " | " + loc + net + " | src=" + ref + " | " + os,
        headers: {
          "Title": src ? "userrequest · " + src : "userrequest",
          "Tags": "chart",
          "Priority": "low"
        },
        keepalive: true
      }).catch(function () {});
    });
})();
