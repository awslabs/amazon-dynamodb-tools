# DynamoDB-ItemSizeCalculator

 **Utility tool to calculate the size of DynamoDB items.**

[![NPM Version][npm-image]][npm-url]
[![Downloads Stats][npm-downloads]][npm-url]

Utility tool to gain item size information in Bytes for DynamoDB JSON items. This allows us to understand capacity consumption and ensure items are under the 400KB DynamoDB item size limit.

DynamoDB SDKs cater for both DDB-JSON and Native JSON. This package can be used to calculate both. By default, it uses DDB-JSON but you can alter methods to take Native JSON by passing boolean value `true` as a parameter to the method:

```js
CalculateSize(item, true)
```


## Installation

OS X & Linux:

```sh
npm install ddb-calc --save
```

## Usage example  
  
### **Require**

```js
const CALC = require('ddb-calc')
```

### **Sample DynamoDB JSON item**

```js
const item = {
        "Id": {
            "N": "101"
        },
        "Title": {
            "S": "Book 101 Title"
        },
        "ISBN": {
            "S": "111-1111111111"
        },
        "Authors": {
            "L": [
                {
                    "S": "Author1"
                }
            ]
        },
        "Price": {
            "N": "2"
        },
        "Dimensions": {
            "S": "8.5 x 11.0 x 0.5"
        },
        "PageCount": {
            "N": "500"
        },
        "InPublication": {
            "BOOL": true
        },
        "ProductCategory": {
            "S": "Book"
        }
    }
```

### **Calculate Size**

```js
const size = CALC.CalculateSize(item);
```

```js
{ 
    rcu: 1, 
    wcu: 1, 
    size: 137 // in Bytes
}
```

### **Understand if an item is under the 400KB limit**

```js
const isValid = CALC.IsUnderLimit(item);
```

### **Sample Native JSON item**

```js
const item = {
    "Id": 101,
    "Title": "Book 101 Title",
    "ISBN": "111-1111111111",
    "Authors": [
        "Author1"
    ],
    "Price": 2,
    "Dimensions": "8.5 x 11.0 x 0.5",
    "PageCount": 500,
    "InPublication": true,
    "ProductCategory": "Book"
}
```

### **Calculate Size**

```js
const size =  CALC.CalculateSize(item, true);
```

```js
{ 
    rcu: 1, 
    wcu: 1, 
    size: 137 // in Bytes
}
```

### **Understand if an item is under the 400KB limit**

```js
const isValid = CALC.IsUnderLimit(item, true);
```

## Release History

* 0.0.4
  * Alter: Native JSON now supported by bool value: `CalculateSizeJson(item, true)`
* 0.0.3
  * ADD: Added native JSON functions `CalculateSizeJson()` and `IsUnderLimitJson()`
* 0.0.2
  * ADD: Added `marshalling` capability for native JSON
* 0.0.1
  * The first proper release
  * ADD: Added `isUnderLimit()` function
* 0.0.0
  * Work in progress

## Contributing

1. Fork it (<https://github.com/awslabs/amazon-dynamodb-tools/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request

<!-- Markdown link & img dfn's -->
[npm-image]: https://img.shields.io/npm/v/ddb-calc.svg?style=flat-square
[npm-url]: https://npmjs.org/package/ddb-calc
[npm-downloads]: https://img.shields.io/npm/dm/ddb-calc.svg?style=flat-square
