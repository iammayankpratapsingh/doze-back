async function nextUserId(account) {
  const suffixBase = "abcdefghijklmnopqrstuvwxyz";
  const used = account.userProfiles?.length || 0;
  return `${account.accountId}${suffixBase[used]}`;
}

module.exports = { nextUserId };