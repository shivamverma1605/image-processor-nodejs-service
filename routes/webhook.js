const express = require('express');
const Request = require('../models/Request');

const router = express.Router();

router.post('/', async (req, res) => {
    try {
        const { requestId, status } = req.body;
        if (!requestId || !status) {
            return res.status(400).json({ error: 'Invalid webhook payload' });
        }

        const request = await Request.findOne({ id: requestId });
        if (!request) {
            return res.status(404).json({ error: 'Request not found' });
        }

        request.status = status;
        request.updatedAt = new Date();
        await request.save();

        res.status(200).json({ message: 'Webhook received and processed' });
    } catch (err) {
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

module.exports = router;