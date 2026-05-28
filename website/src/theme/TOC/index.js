// Swizzle (--wrap): appends a Pacer mascot card under the right-side TOC.
// From the design's template/src/theme/TOC/index.js, with one integration change:
// TOC + Pacer are wrapped in .toc-pinned so the card pins with the TOC in real
// Docusaurus (a plain sibling scrolls away — the inner TOC sticky is neutralized
// in custom.css §21).

import React from 'react';
import TOC from '@theme-original/TOC';
import useBaseUrl from '@docusaurus/useBaseUrl';

export default function TOCWrapper(props) {
  const mascot = useBaseUrl('img/mascot-thinking.svg');
  return (
    <div className="toc-pinned">
      <TOC {...props} />
      <aside className="pacer-card">
        <img src={mascot} alt="" />
        <p>Stuck on something? Pacer triages the issue tracker on Tuesdays.</p>
        <a href="https://github.com/gMountie/gMountie/issues">Open an issue ↗</a>
      </aside>
    </div>
  );
}
