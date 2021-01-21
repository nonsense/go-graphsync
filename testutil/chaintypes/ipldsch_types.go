package chaintypes

// Code generated by go-ipld-prime gengo.  DO NOT EDIT.

import (
	ipld "github.com/ipld/go-ipld-prime"
)

var _ ipld.Node = nil // suppress errors when this dependency is not referenced
// Type is a struct embeding a NodePrototype/Type for every Node implementation in this package.
// One of its major uses is to start the construction of a value.
// You can use it like this:
//
// 		chaintypes.Type.YourTypeName.NewBuilder().BeginMap() //...
//
// and:
//
// 		chaintypes.Type.OtherTypeName.NewBuilder().AssignString("x") // ...
//
var Type typeSlab

type typeSlab struct {
	Block          _Block__Prototype
	Block__Repr    _Block__ReprPrototype
	Bytes          _Bytes__Prototype
	Bytes__Repr    _Bytes__ReprPrototype
	Link           _Link__Prototype
	Link__Repr     _Link__ReprPrototype
	Messages       _Messages__Prototype
	Messages__Repr _Messages__ReprPrototype
	Parents        _Parents__Prototype
	Parents__Repr  _Parents__ReprPrototype
	String         _String__Prototype
	String__Repr   _String__ReprPrototype
}

// --- type definitions follow ---

// Block matches the IPLD Schema type "Block".  It has Struct type-kind, and may be interrogated like map kind.
type Block = *_Block
type _Block struct {
	Parents  _Parents
	Messages _Messages
}

// Bytes matches the IPLD Schema type "Bytes".  It has bytes kind.
type Bytes = *_Bytes
type _Bytes struct{ x []byte }

// Link matches the IPLD Schema type "Link".  It has link kind.
type Link = *_Link
type _Link struct{ x ipld.Link }

// Messages matches the IPLD Schema type "Messages".  It has list kind.
type Messages = *_Messages
type _Messages struct {
	x []_Bytes
}

// Parents matches the IPLD Schema type "Parents".  It has list kind.
type Parents = *_Parents
type _Parents struct {
	x []_Link
}

// String matches the IPLD Schema type "String".  It has string kind.
type String = *_String
type _String struct{ x string }
