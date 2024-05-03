// Source: ChatGPT :sweat_smile:
function parseDuration(durationStr) {
    // Extract the number and the unit from the input string
    const parts = durationStr.match(/(\d+)\s*(year|month|week|day|hour|minute|second)s?/i);

    if (!parts) {
        throw new Error('Invalid duration format');
    }

    const number = parseInt(parts[1], 10);
    const unit = parts[2].toLowerCase();

    // Convert the duration to milliseconds based on the unit
    switch (unit) {
        case 'year':
            return number * 31536000000; // 365 * 24 * 60 * 60 * 1000
        case 'month':
            return number * 2629800000;  // 30.44 * 24 * 60 * 60 * 1000
        case 'week':
            return number * 604800000;   // 7 * 24 * 60 * 60 * 1000
        case 'day':
            return number * 86400000;    // 24 * 60 * 60 * 1000
        case 'hour':
            return number * 3600000;     // 60 * 60 * 1000
        case 'minute':
            return number * 60000;       // 60 * 1000
        case 'second':
            return number * 1000;
        default:
            throw new Error('Invalid time unit');
    }
}

function addMilliseconds(date, millis) {
    var date = new Date(date.valueOf());
    date.setTime(date.getTime() + millis);
    return date;
}

function formatDate(date) {
    const year = date.getFullYear();
    const month = (date.getMonth() + 1).toString().padStart(2, '0'); // Months are zero-indexed
    const day = date.getDate().toString().padStart(2, '0');

    return `DATE '${year}-${month}-${day}'`;
}

function formatTimestamp(date) {
    const year = date.getFullYear();
    const month = (date.getMonth() + 1).toString().padStart(2, '0'); // Months are zero-indexed
    const day = date.getDate().toString().padStart(2, '0');
    const hours = date.getHours().toString().padStart(2, '0');
    const minutes = date.getMinutes().toString().padStart(2, '0');
    const seconds = date.getSeconds().toString().padStart(2, '0');

    return `TIMESTAMP '${year}-${month}-${day} ${hours}:${minutes}:${seconds}'`;
}

export { parseDuration, addMilliseconds, formatDate, formatTimestamp };
