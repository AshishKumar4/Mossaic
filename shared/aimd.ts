import {
  AIMD_INITIAL_CWND,
  AIMD_MAX_CWND,
  AIMD_MIN_CWND,
  AIMD_SSTHRESH,
} from "./constants";

/**
 * AIMD (Additive Increase / Multiplicative Decrease) Concurrency Controller.
 *
 * Manages concurrent HTTP request windows using TCP-style congestion control
 * (Jacobson/Karels RTT estimation per RFC 6298, slow-start + congestion avoidance).
 */
export class AIMDController {
  /** Congestion window (float — floor it for actual concurrency) */
  cwnd: number;
  /** Slow-start threshold */
  ssthresh: number;
  /** Smoothed round-trip time (ms) */
  srtt = 0;
  /** RTT variance */
  rttvar = 0;
  /** Minimum congestion window */
  minCwnd: number;
  /** Maximum congestion window */
  maxCwnd: number;
  /** Whether we've received the first RTT sample */
  private hasFirstSample = false;

  constructor(
    options: {
      initialCwnd?: number;
      ssthresh?: number;
      minCwnd?: number;
      maxCwnd?: number;
    } = {}
  ) {
    this.cwnd = options.initialCwnd ?? AIMD_INITIAL_CWND;
    this.ssthresh = options.ssthresh ?? AIMD_SSTHRESH;
    this.minCwnd = options.minCwnd ?? AIMD_MIN_CWND;
    this.maxCwnd = options.maxCwnd ?? AIMD_MAX_CWND;
  }

  /**
   * Called on a successful chunk transfer.
   * Updates RTT estimates (Jacobson/Karels per RFC 6298) and grows cwnd.
   *
   * Slow-start phase (cwnd < ssthresh): cwnd += 1 per ACK (exponential growth)
   * Congestion avoidance (cwnd >= ssthresh): cwnd += 1/cwnd per ACK (linear growth)
   */
  onSuccess(rttMs: number): void {
    // --- RTT estimation (Jacobson/Karels, RFC 6298 §2) ---
    if (!this.hasFirstSample) {
      // First sample: SRTT = R, RTTVAR = R/2
      this.srtt = rttMs;
      this.rttvar = rttMs / 2;
      this.hasFirstSample = true;
    } else {
      // RTTVAR = 0.75 * RTTVAR + 0.25 * |SRTT - R|
      this.rttvar = 0.75 * this.rttvar + 0.25 * Math.abs(this.srtt - rttMs);
      // SRTT = 0.875 * SRTT + 0.125 * R
      this.srtt = 0.875 * this.srtt + 0.125 * rttMs;
    }

    // --- Congestion window growth ---
    if (this.cwnd < this.ssthresh) {
      // Slow-start: exponential growth (add 1 per ACK)
      this.cwnd += 1;
    } else {
      // Congestion avoidance: linear growth (add 1/cwnd per ACK)
      this.cwnd += 1 / this.cwnd;
    }

    // Clamp to max
    if (this.cwnd > this.maxCwnd) {
      this.cwnd = this.maxCwnd;
    }
  }

  /**
   * Called on a chunk transfer failure (timeout or error).
   * Multiplicative decrease: ssthresh = cwnd/2, cwnd = max(cwnd/2, minCwnd).
   */
  onFailure(): void {
    this.ssthresh = Math.max(this.cwnd / 2, this.minCwnd);
    this.cwnd = Math.max(this.cwnd / 2, this.minCwnd);
  }

  /**
   * Returns the current max concurrency as an integer.
   * floor(cwnd) clamped to [minCwnd, maxCwnd].
   */
  getMaxConcurrency(): number {
    const c = Math.floor(this.cwnd);
    return Math.max(this.minCwnd, Math.min(c, this.maxCwnd));
  }

  /**
   * Compute retransmission timeout: SRTT + 4 * RTTVAR, minimum 1000ms.
   * Returns 1000ms if no RTT samples have been collected yet.
   */
  getRTO(): number {
    if (!this.hasFirstSample) return 1000;
    return Math.max(1000, this.srtt + 4 * this.rttvar);
  }
}
