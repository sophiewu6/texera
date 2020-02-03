
export interface UserAccount extends Readonly<{
  userName: string;
  userID: number;
}> {}

export interface UserAccountResponse extends Readonly<{
  code: number; // 0 represents success and 1 represents error
  userAccount: UserAccount;
}> {}
