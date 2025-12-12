require('dotenv').config();
const { sendNewUserCredentials, verifySmtp } = require('../utils/mailer');

(async () => {
  await verifySmtp();
  await sendNewUserCredentials('your-test-email@domain.com', 'Test User', 'Temp1234!');
  console.log('Sent!');
})();
