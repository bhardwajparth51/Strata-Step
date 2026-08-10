import type { ChargeResult, PaymentAdapter } from './paymentAdapter.js';

/**
 * StripeAdapter — in-process simulation of the Stripe Charges API.
 *
 * Implements PaymentAdapter so callers depend only on the interface.
 * A real implementation would swap this class for one that calls
 * stripe.charges.create() with the same signature.
 *
 * Idempotency guarantee: identical idempotencyKey returns the cached
 * result without invoking a second external call — matching Stripe's
 * server-side idempotency key behaviour.
 *
 * AbortSignal: callers may pass an AbortSignal to cancel an in-flight
 * charge attempt (e.g. when the durable lock is lost mid-execution).
 * The adapter checks the signal before and during the simulated network
 * round-trip so the abort path is exercisable in tests.
 */
export class StripeAdapter implements PaymentAdapter {
  private processedCharges = new Map<string, ChargeResult>();
  public externalChargeCount = 0;
  public totalRequestsHandled = 0;

  async createCharge(
    amount: number,
    currency: string,
    idempotencyKey: string,
    signal?: AbortSignal
  ): Promise<ChargeResult> {
    this.totalRequestsHandled++;

    // Idempotency hit — instant return from local cache, no external call.
    if (this.processedCharges.has(idempotencyKey)) {
      console.log(`[stripe adapter] idempotency hit for '${idempotencyKey}'`);
      const existing = this.processedCharges.get(idempotencyKey)!;
      return { ...existing, reusedExistingCharge: true };
    }

    // Check before the simulated network round-trip.
    if (signal?.aborted) {
      throw new DOMException('Charge aborted before external API call', 'AbortError');
    }

    // Simulate network latency so the AbortSignal can fire mid-flight.
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(resolve, 20);
      signal?.addEventListener(
        'abort',
        () => {
          clearTimeout(timer);
          reject(new DOMException('Charge cancelled mid-flight by AbortSignal', 'AbortError'));
        },
        { once: true }
      );
    });

    this.externalChargeCount++;
    const chargeId = `ch_simulated_${Math.random().toString(36).substring(2, 9)}`;
    const result: ChargeResult = {
      chargeId,
      amount,
      currency,
      status: 'paid',
      idempotencyKey,
      reusedExistingCharge: false,
    };

    this.processedCharges.set(idempotencyKey, result);
    console.log(
      `[stripe adapter] charge created: ${chargeId} ($${amount / 100}) key: ${idempotencyKey}`
    );
    return result;
  }
}
