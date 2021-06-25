require('dotenv').config();
const fs = require('fs');
const { BigQuery } = require('@google-cloud/bigquery');
const nunjucks = require('nunjucks');
const nodemailer = require('nodemailer');

const bqClient = new BigQuery({
  scopes: [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/drive',
  ],
});

const DATASET = 'curb_tracking';

const get = async () => {
  const query = `SELECT * FROM ${DATASET}._EmailAlert`;
  const queryOptions = {
    query,
  };
  const [rows] = await bqClient.query(queryOptions);
  return rows;
};

const compose = async (rows) => {
  nunjucks.configure('views', { autoescape: true });
  const html = nunjucks.render('reports.html', { rows });
  return html;
};

const sendEmail = async (html) => {
  const transporter = nodemailer.createTransport({
    service: 'gmail',
    host: 'smtp.gmail.com',
    port: 465,
    secure: true,
    auth: {
      user: 'siddhantmehandru.developer@gmail.com',
      pass: process.env.APP_PWD,
    },
  });

  const info = await transporter.sendMail({
    from: '"Sid Dev" <siddhantmehandru.developer@gmail.com>',
    to: 'hieumdd@gmail.com, jhamb285@gmail.com',
    subject: 'Hello ✔',
    text: 'Hello world?',
    html,
  });

  info;

  console.log('Message sent: %s', info.messageId);
};

const run = async () => {
  const rows = await get();
  const html = await compose(rows);
  await sendEmail(html);
};

(async () => {
  await run();
})();
