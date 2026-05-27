import React from 'react';
import TOC from '@theme-original/TOC';

// Wraps the stock right-column TOC and appends the "Pacer" mascot card from the
// docs UI kit (community nudge). Wrapping (not ejecting) keeps us on the upstream
// component so it survives Docusaurus upgrades.
export default function TOCWrapper(props) {
  return (
    <>
      <TOC {...props} />
      <div className="pacer-card">
        <img src="/img/mascot-thinking.svg" alt="" width={92} height={92} />
        <p>Stuck on something? Open an issue — the maintainers triage weekly.</p>
        <a href="https://github.com/gMountie/gMountie/issues">Open an issue →</a>
      </div>
    </>
  );
}
