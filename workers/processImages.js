const mongoose = require('mongoose');
const { mongoURI } = require('../config');
const { processImages } = require('../services/imageProcessor');
const Request = require('../models/Request');

mongoose.connect(mongoURI)
    .then(() => console.log('MongoDB connected'))
    .catch(err => console.log(err));

async function startProcessing() {
    const pendingRequests = await Request.find({ status: 'PENDING' });
    for (const request of pendingRequests) {
        await Request.updateOne({ id: request.id }, { status: 'PROCESSING', updatedAt: new Date() });
        processImages(request.id);
    }
}

startProcessing();