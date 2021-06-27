const run = require('./models');

exports.main = async (req, res) => {
    const results = await run();
    res.status(200).send(results);
  };
