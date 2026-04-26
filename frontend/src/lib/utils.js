import { clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

/**
 * Class-name combiner used by every shadcn-ui generated component.
 *
 * `clsx` flattens the arg list (string | object | array | falsy) into a
 * single space-separated class string; `twMerge` then resolves Tailwind
 * conflicts (e.g. `px-2 px-4` -> `px-4`) so callers can layer overrides
 * without worrying about precedence.
 */
export function cn(...inputs) {
  return twMerge(clsx(inputs))
}
