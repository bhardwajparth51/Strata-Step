export class NonRetryableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NonRetryableError';
    Object.setPrototypeOf(this, NonRetryableError.prototype);
  }
}

export class LockAcquisitionTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'LockAcquisitionTimeoutError';
    Object.setPrototypeOf(this, LockAcquisitionTimeoutError.prototype);
  }
}

export class LockLostError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'LockLostError';
    Object.setPrototypeOf(this, LockLostError.prototype);
  }
}

export class ConcurrentStateMutationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConcurrentStateMutationError';
    Object.setPrototypeOf(this, ConcurrentStateMutationError.prototype);
  }
}
