import { describe, expect, it } from 'vitest';
import { backoffDelay, MAX_BACKOFF_MS } from '../src/core/backoff.js';

describe('backoffDelay', () => {
  it('doubles with each attempt', () => {
    const max = () => 1; // jitter at its maximum
    expect([1, 2, 3, 4, 5].map((a) => backoffDelay(a, 1000, max))).toEqual([1000, 2000, 4000, 8000, 16000]);
  });

  it('jitters between 50% and 100% of the exponential delay', () => {
    expect(backoffDelay(3, 1000, () => 0)).toBe(2000);
    expect(backoffDelay(3, 1000, () => 0.5)).toBe(3000);
    for (let i = 0; i < 200; i++) {
      const d = backoffDelay(4, 1000);
      expect(d).toBeGreaterThanOrEqual(4000);
      expect(d).toBeLessThanOrEqual(8000);
    }
  });

  it('never waits longer than the cap', () => {
    expect(backoffDelay(40, 1000, () => 1)).toBe(MAX_BACKOFF_MS);
  });
});
