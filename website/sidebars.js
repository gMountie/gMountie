// @ts-check
// Hand-authored sidebar, ported from the old docsify `docs/_sidebar.md`.
// Doc IDs are file paths under ../docs without the extension.

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'home', // docs/index.md — slug "/"
    'quickstart',
    {
      type: 'category',
      label: 'Server',
      items: ['server/cli', 'server/config'],
    },
    {
      type: 'category',
      label: 'Client',
      items: ['client/cli', 'client/config'],
    },
    'roadmap',
    {
      type: 'category',
      label: 'Design',
      items: [
        'design/architecture',
        'design/caching-and-consistency',
        'design/performance',
        'design/identity-and-permissions',
        'design/operations-and-packaging',
      ],
    },
  ],
};

export default sidebars;
