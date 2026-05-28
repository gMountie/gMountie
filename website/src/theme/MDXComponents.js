// Swizzle: register custom components globally in MDX.
// Lets you write <Pacer>…</Pacer> in any .md/.mdx without an import.

import MDXComponents from '@theme-original/MDXComponents';
import Pacer from '@site/src/components/Pacer';

export default {
  ...MDXComponents,
  Pacer,
};
