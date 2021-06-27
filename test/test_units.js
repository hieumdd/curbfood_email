const assert = require("chai").assert;
const sinon = require("sinon");

const { main } = require("..");

describe("Unit Test", () => {
  it("auto", async () => {
    const req = {
      day: 1,
    };
    const res = {
      send: sinon.stub(),
      status(s) {
        this.statusCode = s;
        return this;
      },
    };
    await main(req, res);
    assert.isTrue(res.send.calledOnce);
  }).timeout(0);
});
