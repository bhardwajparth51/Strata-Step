/**
 * PaymentAdapter — interface contract for any payment processor.
 * Concrete implementations live in their own adapter files.
 */
export interface ChargeResult {
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
    idempotencyKey: string,
    signal?: AbortSignal
  ): Promise<ChargeResult>;
}
