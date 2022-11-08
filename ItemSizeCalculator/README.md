# DynamoDB-ItemSizeCalculator
> Utility tool to calculate the size of a DynamoDB items.

[![NPM Version][npm-image]][npm-url]
[![Downloads Stats][npm-downloads]][npm-url]

Utility tool to gain item size information for DynamoDB JSON items to understand capacity consumption and ensure items are under the 400KB DynamoDB limit.

DynamoDB SDK's can cater for both DDB-JSON or Native JSON. This package can be used to calculate both. By default it uses DDB-JSON but you can alter the methods to take Native JSON by passing a bool `true` value as a parameter to the method:
`CalculateSize(item, true)`

![](https://www.cdata.com/blog/articles/20191018-dynamodb-performance-0.png)

## Installation

OS X & Linux:

```sh
npm install ddb-calc --save
```


## Usage example  
  
### **Require**
 ```
const CALC = require('ddb-calc')
 ```

### **Sample DynamoDB JSON item**
```
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
```
const size =  CALC.CalculateSize(item);
```
```
{ 
    rcu: 1, 
    wcu: 1, 
    size: 137 
}
```

### **Understand if an item is under the 400Kb limit**
```
const isValid = CALC.IsUnderLimit(item);
```
```
true
```
_For more examples and usage, please refer to the [Wiki][wiki]._


### **Sample Native JSON item**
```
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
```
const size =  CALC.CalculateSize(item, true);
```

```
{ 
    rcu: 1, 
    wcu: 1, 
    size: 137 
}
```

### **Understand if an item is under the 400Kb limit**
```
const isValid = CALC.IsUnderLimit(item, true);
```
```
true
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

## Meta

Lee Hannigan – [@leeroyhannigan](https://twitter.com/leeroyhannigan) – leeroy.hannigan@gmail.com

[https://github.com/leeroyhannigan/DynamoDB-ItemSizeCalculator](https://github.com/leeroyhannigan/)

## Contributing

1. Fork it (<https://github.com/leeroyhannigan/DynamoDB-ItemSizeCalculator/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request

<!-- Markdown link & img dfn's -->
[npm-image]: https://img.shields.io/npm/v/ddb-calc.svg?style=flat-square
[npm-url]: https://npmjs.org/package/ddb-calc
[npm-downloads]: https://img.shields.io/npm/dm/ddb-calc.svg?style=flat-square
[wiki]: https://github.com/leeroyhannigan/DynamoDB-ItemSizeCalculator/wiki
