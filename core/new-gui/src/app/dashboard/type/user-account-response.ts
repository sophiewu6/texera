import { UserAccount } from './user-account';

export interface UserAccountResponse extends Readonly<{
  code: number; // 0 represents success and 1 represents error
  userAccount: UserAccount;
}> {}

