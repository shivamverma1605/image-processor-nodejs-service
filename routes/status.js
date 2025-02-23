const express = require('express');
const Request = require('../models/Request');

const router = express.Router();

router.get('/:requestId', async (req, res) => {
    try {
        const { requestId } = req.params;
        const request = await Request.findOne({ id: requestId });
        if (!request) {
            return res.status(404).json({ error: 'Request not found' });
        }
        res.status(200).json({ requestId, status: request.status });
    } catch (err) {
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

module.exports = router;