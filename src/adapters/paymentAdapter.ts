export interface PaymentChargeResult {
  chargeId: string;
  amount: number;
  currency: string;
  status: string;
  idempotencyKey: string;
  reusedExistingCharge: boolean;
}

export interface PaymentAdapter {
  createCharge(
    amount: number,
    currency: string,
    idempotencyKey: string
  ): Promise<PaymentChargeResult>;
}

export class FakeStripeAdapter implements PaymentAdapter {
  private processedCharges = new Map<string, PaymentChargeResult>();
  public externalChargeCount: number = 0;
  public totalRequestsHandled: number = 0;

  async createCharge(
    amount: number,
    currency: string,
    idempotencyKey: string
  ): Promise<PaymentChargeResult> {
    this.totalRequestsHandled++;

    if (this.processedCharges.has(idempotencyKey)) {
      console.log(`[payment adapter] idempotency hit for '${idempotencyKey}'`);
      const existing = this.processedCharges.get(idempotencyKey)!;
      return {
        ...existing,
        reusedExistingCharge: true,
      };
    }

    this.externalChargeCount++;
    const chargeId = `ch_simulated_${Math.random().toString(36).substring(2, 9)}`;
    const newCharge: PaymentChargeResult = {
      chargeId,
      amount,
      currency,
      status: 'paid',
      idempotencyKey,
      reusedExistingCharge: false,
    };

    this.processedCharges.set(idempotencyKey, newCharge);
    console.log(`[payment adapter] charge created: ${chargeId} ($${amount / 100}) key: ${idempotencyKey}`);
    return newCharge;
  }
}
