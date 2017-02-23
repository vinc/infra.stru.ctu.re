const express = require('express');
const crypto = require('crypto');
const mkdirp = require('mkdirp');
const morgan = require('morgan');
const sharp = require('sharp');
const path = require('path');
const url = require('url');
const fs = require('fs');

const LargeObjectManager = require('pg-large-object').LargeObjectManager;
const Promise = require('bluebird');
const pgp = require('pg-promise')({ promiseLib: Promise });

Promise.promisifyAll(LargeObjectManager.prototype, {multiArgs: true});

if (typeof process.env.DATABASE_URL === 'undefined') {
  throw new ReferenceError('DATABASE_URL environment variable must be set');
}
const db = pgp(process.env.DATABASE_URL);

var charges = [];
var lastChargedAt = +new Date();

const chargeImage = function(token, length) {
  charges.push([token, length]);

  setTimeout(function() {
    const now = +new Date();
    if (now - lastChargedAt > 1000) {
      lastChargedAt = now;

      const n = charges.length;

      // Sum lengths of identical tokens
      const chargesAcc = charges.splice(0).reduce(function(acc, [k, v]) { acc[k] = (acc[k] || 0) + v; return acc }, { });

      console.log('LOG Charging ' + n + ' request(s) for ' + Object.keys(chargesAcc).length + ' image(s)');

      const sql = 'UPDATE users SET balance = balance - $2 FROM pictures WHERE token = $1 AND users.id = pictures.user_id';
      const query = pgp.helpers.concat(Object.entries(chargesAcc).map(charge => ({ query: sql, values: charge })));

      db.none(query).catch(function(err) {
        console.error('Error updating user balance', err);
      });
    }
  }, 1000);
};

const cacheDir = process.env.CACHE_DIR || 'tmp';

const oidSQL = function(params) {
  switch (params.model + '_' + params.attribute) {
  case 'picture_image':
    return 'SELECT image AS oid FROM pictures WHERE token = ${id} AND image_filename = ${filename}';
  case 'user_avatar':
    return 'SELECT avatar AS oid FROM users WHERE username = ${id} AND avatar_filename = ${filename}';
  }
}

const cacheFile = function(req, res, next) {
  req.cache = path.join(cacheDir, req.path);
  req.originalCache = path.join(cacheDir, ...['model', 'attribute', 'id', 'filename'].map(k => req.params[k]))
  if (req.originalCache != req.cache) {
    req.resizedCache = req.cache;
  }
  req.tempCache = path.join(cacheDir, crypto.randomBytes(24).toString('hex') + '.jpg');

  fs.stat(req.cache, function(err, stats) {
    if (stats) {
      if (err) {
        return next(err);
      } else {
        return serveFile(req, res); // Bypass next() functions
      }
    }

    mkdirp(path.dirname(req.cache), function(err) {
      return next(err);
    });
  });
};

const streamFile = function(req, res, next) {
  var connection;

  db.connect().then(cn => {
    connection = cn;
    return cn.tx(t => {
      return t.one(oidSQL(req.params), req.params).then(function(image) {
        const man = new LargeObjectManager(cn.client);
        return man.openAndReadableStreamAsync(image.oid).then(function([size, stream]) {
          const temp = fs.createWriteStream(req.tempCache);

          stream.pipe(temp);

          return new Promise(function(resolve) {
            stream.on('end', function() {
              fs.rename(req.tempCache, req.originalCache, function(err) {
                if (err) {
                  return next(err);
                }
                resolve();
              });
            });
          });
        });
      });
    }).then(next).catch(next).finally(function() {
      if (connection) {
        connection.done();
      }
    });
  });
};

const resizeFile = function(req, res, next) {
  var geometry = req.params.geometry;
  var crop = false;

  if (geometry.slice(-1) == '!') {
    geometry = geometry.slice(0, -1);
    crop = true;
  }

  const dimensions = geometry.split('x');
  const width = Number(dimensions[0]) || null;
  const height = Number(dimensions[1]) || null;

  var resizer = sharp(req.originalCache).resize(width, height);

  if (!crop) {
    resizer = resizer.max();
  }

  resizer
    .sharpen()
    .jpeg({ quality: 90 })
    .toFile(req.tempCache, function(err, info) {
      if (err) {
        return next(err);
      }

      fs.rename(req.tempCache, req.resizedCache, next);
    });
};

const serveFile = function(req, res, next) {
  fs.readFile(req.cache, function(err, data) {
    if (err) {
      return next(err);
    }
    if (req.params.model == 'picture') {
      chargeImage(req.params.id, data.length);
    }
    res.setHeader('Content-Type',   'image/jpeg');
    res.setHeader('Content-Length', data.length);
    res.send(data);
  });
};

const app = express();

app.enable('trust proxy');
app.use(morgan('dev'))

app.get('/:model/:attribute/:id/:filename', cacheFile, streamFile, serveFile);

app.get('/:model/:attribute/:id/:geometry/:filename', cacheFile, streamFile, resizeFile, serveFile);

app.use(express.static('public'));

const errorPage = fs.readFileSync(path.join(__dirname, 'public/index.html'));

app.use(function(req, res) {
  res.writeHead(404, { 'Content-Type': 'text/html' });
  res.write(errorPage);
  res.end();
});

app.use(function(err, req, res, next) {
  console.error(err);
  res.writeHead(500, { 'Content-Type': 'text/html' });
  res.write(errorPage);
  res.end();
});

app.listen(process.env.PORT || '3000');
