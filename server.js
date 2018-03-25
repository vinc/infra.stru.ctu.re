const express = require('express');
const crypto = require('crypto');
const mkdirp = require('mkdirp');
const morgan = require('morgan');
const sharp = require('sharp');
const path = require('path');
const url = require('url');
const fs = require('fs');

const LargeObjectManager = require('pg-large-object').LargeObjectManager;
const pgp = require('pg-promise')();
const db = pgp(process.env.DATABASE_URL || 'postgres://localhost:5432/picture_development');

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
      const chargesAcc = charges.splice(0).reduce(function(acc, [k, v]) {
        acc[k] = (acc[k] || 0) + v;
        return acc;
      }, {});

      console.log('LOG Charging ' + n + ' request(s) for ' + Object.keys(chargesAcc).length + ' image(s)');

      const sql = 'UPDATE users SET balance = balance - $2 FROM pictures WHERE token = $1 AND users.id = pictures.user_id';
      const query = pgp.helpers.concat(Object.entries(chargesAcc).map(charge => ({
        query: sql,
        values: charge
      })));

      db.none(query).catch(function(err) {
        console.error('Error updating user balance', err);
      });
    }
  }, 1000);
};

const cacheDir = process.env.CACHE_DIR || 'tmp';

const oidSQL = function(params) {
  switch (params.model) {
  case 'pictures':
    return 'SELECT image AS oid FROM pictures WHERE token = ${id} AND image_filename = ${filename}';
  case 'users':
    return 'SELECT avatar AS oid FROM users WHERE username = ${id} AND avatar_filename = ${filename}';
  }
};

const cacheFile = function(req, res, next) {
  req.cache = path.join(cacheDir, req.path);
  req.originalCache = path.join(cacheDir, ...['model', 'id', 'filename'].map(k => req.params[k]));
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
  db.tx(tx => {
    return tx.one(oidSQL(req.params), req.params).then(function(image) {
      const man = new LargeObjectManager({ pgPromise: tx });
      const bufferSize = 16384;

      return man.openAndReadableStreamAsync(image.oid, bufferSize).then(function([size, stream]) {
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
  }).then(next).catch(next);
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
app.use(morgan('dev'));

// New routes:
// v1: /picture/image/3cfbb86a/x300/7919403e579249e288fcfae3926e1e04.jpg
// v2: /pictures/3cfbb86a/x300/7919403e579249e288fcfae3926e1e04.jpg
app.get('/:model/:attribute(image|avatar)/*', function(req, res) {
  res.redirect('/' + req.params.model + 's/' + req.params[0]);
});

app.get('/:model/:id/:filename', cacheFile, streamFile, serveFile);
app.get('/:model/:id/:geometry/:filename', cacheFile, streamFile, resizeFile, serveFile);

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

app.listen(process.env.PORT || '4000');
