// Swizzle: replace Docusaurus's icon color-mode toggle with the template's
// rectangular, labeled, two-state button ("Light" / "Dark"). Styled via
// .theme-toggle in custom.css §22.

import React from 'react';
import clsx from 'clsx';

export default function ColorModeToggle({className, value, onChange}) {
  const isDark = value === 'dark';
  const next = isDark ? 'light' : 'dark';
  return (
    <button
      type="button"
      className={clsx('clean-btn', 'theme-toggle', className)}
      onClick={() => onChange(next)}
      title={`Switch to ${next} mode`}
      aria-label={`Switch to ${next} mode`}>
      {isDark ? 'Dark' : 'Light'}
    </button>
  );
}
