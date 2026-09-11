KaTeX 0.16.11 — https://katex.org  ·  MIT-Lizenz, siehe LICENSE

Lokale Kopie aus dem npm-Paket katex@0.16.11, damit die Seite keine fremden
CDNs lädt. Enthalten sind nur die Dateien, die die Seite braucht:

  katex.min.css    Stylesheet, unverändert
  katex.min.js     Renderer, unverändert
  fonts/*.woff2    Schriften

Aus dem Paket weggelassen sind die woff- und ttf-Fassungen der Schriften
sowie die contrib-Erweiterungen. In der @font-face-Regel steht woff2 an
erster Stelle, alle aktuellen Browser laden deshalb nur diese Dateien.

Aktualisieren: neue Version von https://registry.npmjs.org/katex entpacken
und dist/katex.min.css, dist/katex.min.js sowie dist/fonts/*.woff2 hierher
kopieren.
