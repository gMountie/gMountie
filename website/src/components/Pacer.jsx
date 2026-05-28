import React from 'react';
import useBaseUrl from '@docusaurus/useBaseUrl';

/**
 * <Pacer> — the marmot mascot in a small card.
 * Usable globally in MDX (registered via src/theme/MDXComponents.js).
 *
 *   <Pacer>Stuck? Pacer triages the issue tracker on Tuesdays.</Pacer>
 *   <Pacer pose="pointing" action="Open an issue" href="https://github.com/...">
 *     Something looks off?
 *   </Pacer>
 *   <Pacer inline pose="confused">
 *     If readdir returns EIO, check `gmountie status` first.
 *   </Pacer>
 *
 * pose:   thinking (default) | pointing | confused | sleeping | waving
 * inline: renders the card horizontally inside prose
 */
const POSES = {
  thinking: 'img/mascot-thinking.svg',
  pointing: 'img/mascot-pointing.svg',
  confused: 'img/mascot-confused.svg',
  default:  'img/mascot.svg',
};

export default function Pacer({ pose = 'thinking', inline = false, action, href, children }) {
  const src = useBaseUrl(POSES[pose] || POSES.thinking);
  const cls = inline ? 'pacer-card pacer-card--inline' : 'pacer-card';
  return (
    <aside className={cls}>
      <img src={src} alt="" />
      <div>
        <p>{children}</p>
        {action && href && (
          <a href={href} style={{ marginTop: 6, display: 'inline-block' }}>
            {action} ↗
          </a>
        )}
      </div>
    </aside>
  );
}
