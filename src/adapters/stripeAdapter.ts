/**
 * Stripe adapter implementation supporting idempotency key lookup.
 */
export interface StripeChargeResult {
  chargeId: string;
  amount: number;
  currency: string;
  status: string;
  idempotencyKey: string;
  reusedExistingCharge: boolean;
}

export class StripeAdapter {
  private processedCharges = new Map<string, StripeChargeResult>();
  public externalChargeCount: number = 0;
  public totalRequestsHandled: number = 0;

  async createCharge(
    amount: number,
    currency: string,
    idempotencyKey: string
  ): Promise<StripeChargeResult> {
    this.totalRequestsHandled++;

    if (this.processedCharges.has(idempotencyKey)) {
      console.log(`[stripe adapter] idempotency hit for '${idempotencyKey}'`);
      const existing = this.processedCharges.get(idempotencyKey)!;
      return {
        ...existing,
        reusedExistingCharge: true,
      };
    }

    this.externalChargeCount++;
    const chargeId = `ch_simulated_${Math.random().toString(36).substring(2, 9)}`;
    const newCharge: StripeChargeResult = {
      chargeId,
      amount,
      currency,
      status: 'paid',
      idempotencyKey,
      reusedExistingCharge: false,
    };

    this.processedCharges.set(idempotencyKey, newCharge);
    console.log(`[stripe adapter] charge created: ${chargeId} ($${amount / 100}) key: ${idempotencyKey}`);
    return newCharge;
  }
}

export const globalStripeAdapter = new StripeAdapter();
