package main

import (
	"bufio"
	"container/list"
	"fmt"
	"go/types"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
)

const GeneratedFileSuffix = "_ydbgen"

var (
	sdkImportPath = "github.com/ydb-platform/ydb-go-sdk/v3"

	deps = []dep{
		{
			path:  "strconv",
			ident: "strconv.Itoa",
			std:   true,
		},
		{
			path:  sdkImportPath,
			ident: "ydb.StringValue",
		},
		{
			path:  sdkImportPath + "/table",
			ident: "table.NewQueryParameters",
		},
	}
)

type dep struct {
	path  string
	ident string
	std   bool
}

func tab(n int) string {
	return strings.Repeat("\t", n)
}

type scope struct {
	vars map[string]bool
}

func (s *scope) has(name string) bool {
	if s.vars == nil {
		return false
	}
	return s.vars[name]
}

func (s *scope) add(name string) {
	if s.vars == nil {
		s.vars = make(map[string]bool)
	}
	if s.vars[name] {
		panic("scope: variable already exists")
	}
	s.vars[name] = true
}

type Generator struct {
	once        sync.Once
	conversions map[string]ConversionTemplate
	seenConv    map[string]bool

	scopes list.List // list<scope>
}

func (g *Generator) enterScope() {
	g.scopes.PushFront(new(scope))
}

func (g *Generator) leaveScope() {
	g.scopes.Remove(g.scopes.Front())
}

func (g *Generator) declare(v string) string {
	return g.doDeclare(v, false)
}

func (g *Generator) declareShort(v string) string {
	return g.doDeclare(v, true)
}

func (g *Generator) doDeclare(v string, short bool) string {
	var (
		front = g.scopes.Front()
		curr  = front.Value.(*scope)
		x     = 0
	)
search:
	for {
		var name string
		if x == 0 && short {
			name = v
		} else {
			name = v + strconv.Itoa(x)
		}
		for el := front; el != nil; el = el.Next() {
			s := el.Value.(*scope)
			if s.has(name) {
				x++
				continue search
			}
		}
		curr.add(name)
		return name
	}
}

func (g *Generator) init() {
	g.once.Do(func() {
		g.conversions = make(map[string]ConversionTemplate)
		g.seenConv = make(map[string]bool)
	})
}

func (g *Generator) importDeps(bw *bufio.Writer) {
	_, _ = bw.WriteString("import (\n")
	prev := deps[0].std
	for _, dep := range deps {
		if dep.std != prev {
			prev = dep.std
			line(bw)
		}
		line(bw, tab(1), `"`, dep.path, `"`)
	}
	line(bw, `)`)
	line(bw)

	// Suppress unused imports.
	line(bw, "var (")
	for _, dep := range deps {
		if dep.ident != "" {
			line(
				bw,
				tab(1),
				`_ = `,
				dep.ident,
			)
		}
	}
	line(bw, ")")
	line(bw)
}

func (g *Generator) fileHeader(bw *bufio.Writer, pkg string) {
	line(bw, `// Code generated by ydbgen; DO NOT EDIT.`)
	line(bw)
	line(bw, `package `, pkg)
	line(bw)
}

func (g *Generator) Generate(pkg Package) error {
	g.init()

	for _, p := range pkg.Pipelines {
		if p.f.Empty() {
			continue
		}

		bw := bufio.NewWriter(p.w())

		g.fileHeader(bw, pkg.Name)
		_ = bw.Flush() // Omit `... found EOF want package` error on panics.

		g.importDeps(bw)

		for _, s := range p.f.Structs {
			if s.Flags&GenScan != 0 {
				g.generateStructScan(bw, s)
			}
			if s.Flags&GenQueryParams != 0 {
				g.generateStructQueryParams(bw, s)
			}
			if s.Flags&GenValue != 0 {
				g.generateStructValue(bw, s)
			}
			if s.Flags&GenType != 0 {
				g.generateStructType(bw, s)
			}
			log.Printf("%s struct", s.Name)
		}
		for _, s := range p.f.Slices {
			if s.Flags&GenScan != 0 {
				g.generateSliceScan(bw, s)
			}
			if s.Flags&GenType != 0 {
				g.generateSliceType(bw, s)
			}
			if s.Flags&GenValue != 0 {
				g.generateSliceValue(bw, s)
			}
			log.Printf("%s slice", s.Name)
		}
		var templates []string
		for name := range g.conversions {
			if !g.seenConv[name] {
				templates = append(templates, name)
			}
		}
		sort.Slice(templates, func(i, j int) bool {
			return templates[i] < templates[j]
		})
		for _, name := range templates {
			g.conversions[name].Write(bw)
			delete(g.conversions, name)
			g.seenConv[name] = true
		}
		if err := bw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func structReceiver(s *Struct) string {
	return strings.ToLower(s.Name[0:1])
}

func sliceReceiver(s *Slice) string {
	return strings.ToLower(s.Name[0:1]) + "s"
}

func (g *Generator) generateStructScan(bw *bufio.Writer, s *Struct) {
	g.enterScope()
	defer g.leaveScope()
	var (
		rcvr = g.declareShort(structReceiver(s))
		res  = g.declareShort("res")
		err  = g.declareShort("err")
	)
	code(bw, `func (`, rcvr, ` *`, s.Name+`) `)
	line(bw, `Scan(`, res, ` *table.Result) (`, err, ` error) {`)
	g.writeStructFieldsScan(bw, 1, res, rcvr, s)
	line(bw, tab(1), `return `, res, `.err()`)
	line(bw, `}`)
	line(bw)
}

func (g *Generator) generateStructContainerScan(bw *bufio.Writer, s *Struct) {
	g.enterScope()
	defer g.leaveScope()

	var (
		rcvr = g.declareShort(structReceiver(s))
		res  = g.declareShort("res")
		err  = g.declareShort("err")
	)
	code(bw, `func (`, rcvr, ` *`, s.Name+`) `)
	line(bw, `ScanContainer(`, res, ` *table.Result) (`, err, ` error) {`)
	g.writeStructContainerScan(bw, 1, res, rcvr, s)
	line(bw, tab(1), `return `, res, `.err()`)
	line(bw, `}`)
	line(bw)
}

func (g *Generator) generateStructQueryParams(bw *bufio.Writer, s *Struct) {
	g.enterScope()
	defer g.leaveScope()

	var (
		rcvr = g.declareShort(structReceiver(s))
	)
	code(bw, `func (`, rcvr, ` *`, s.Name+`) `)
	line(bw, `QueryParameters() *table.QueryParameters {`)

	writeField := g.structFieldWriter(bw, 1, rcvr, s)
	line(bw, tab(1), `return table.NewQueryParameters(`)
	for _, f := range s.Fields {
		code(bw, tab(2))
		code(bw, `table.ValueParam("$`, f.Column, `", `)
		writeField(f)
		line(bw, `),`)
	}
	line(bw, tab(1), ")")
	line(bw, "}")
	line(bw)
}

func (g *Generator) generateStructValue(bw *bufio.Writer, s *Struct) {
	g.enterScope()
	defer g.leaveScope()

	rcvr := g.declareShort(structReceiver(s))
	code(bw, `func (`, rcvr, ` *`, s.Name+`) `)
	line(bw, `StructValue() ydb.Value {`)
	val := g.assignStructValue(bw, 1, rcvr, s)
	line(bw, tab(1), `return `, val)
	line(bw, `}`)
	line(bw)
}

func (g *Generator) generateStructType(bw *bufio.Writer, s *Struct) {
	g.enterScope()
	defer g.leaveScope()

	var (
		rcvr = g.declareShort(structReceiver(s))
	)
	code(bw, `func (`, rcvr, ` *`, s.Name+`) `)
	line(bw, `StructType() ydb.Type {`)
	typ := g.assignStructType(bw, 1, s)
	line(bw, tab(1), `return `, typ)
	line(bw, `}`)
	line(bw)
}

func (g *Generator) generateSliceScan(bw *bufio.Writer, s *Slice) {
	g.enterScope()
	defer g.leaveScope()

	var (
		rcvr = g.declareShort(sliceReceiver(s))
		res  = g.declareShort("res")
		err  = g.declareShort("err")
	)
	code(bw, `func (`, rcvr, ` *`, s.Name+`) `)
	line(bw, `Scan(`, res, ` *table.Result) (`, err, ` error) {`)

	g.enterScope()
	{
		line(bw, tab(1), `for `, res, `.NextRow() {`)
		x := g.declare("x")
		line(bw, tab(2), `var `, x, ` `, goTypeString(s.T))
		g.writeScan(bw, 2, res, x, s.T)

		line(bw, tab(2), `if `, res, `.err() == nil {`)
		line(bw, tab(3), `*`, rcvr, ` = append(*`, rcvr, `, `, x, `)`)
		line(bw, tab(2), `}`)
		line(bw, tab(1), `}`)
	}
	g.leaveScope()

	line(bw, tab(1), `return `, res, `.err()`)
	line(bw, `}`)
	line(bw)
}

func (g *Generator) generateSliceValue(bw *bufio.Writer, s *Slice) {
	g.enterScope()
	defer g.leaveScope()

	rcvr := g.declareShort(sliceReceiver(s))
	code(bw, `func (`, rcvr, ` `, s.Name+`) `)
	line(bw, `ListValue() ydb.Value {`)
	list := g.assignSliceValue(bw, 1, rcvr, s)
	line(bw, tab(1), `return `, list)
	line(bw, `}`)
	line(bw)
}

func (g *Generator) assignSliceValue(bw *bufio.Writer, depth int, rcvr string, s *Slice) string {
	var (
		list   = g.declare("list")
		values = g.declare("vs")
	)

	line(bw, tab(depth), `var `, list, ` ydb.Value`)
	line(bw, tab(depth), values, ` := make([]ydb.Value, len(`, rcvr, `))`)

	g.enterScope()
	defer g.leaveScope()

	var (
		i    = g.declare("i")
		item = g.declare("item")
	)
	line(bw, tab(depth), `for `, i, `, `, item, ` := range `, rcvr, ` {`)
	r := g.assignValue(bw, depth+1, item, s.T)
	line(bw, tab(depth+1), values, `[`, i, `] = `, r)
	line(bw, tab(depth), `}`)

	line(bw, tab(depth), `if len(`, values, `) == 0 {`)
	t := g.assignSliceType(bw, depth+1, s)
	line(bw, tab(depth+1), list, ` = ydb.ZeroValue(`, t, `)`)
	line(bw, tab(depth), `} else {`)
	line(bw, tab(depth+1), list, ` = ydb.ListValue(`, values, `...)`)
	line(bw, tab(depth), `}`)

	return list
}

func (g *Generator) generateSliceType(bw *bufio.Writer, s *Slice) {
	g.enterScope()
	defer g.leaveScope()

	code(bw, `func (`, s.Name+`) `)
	line(bw, `ListType() ydb.Type {`)
	t := g.assignSliceType(bw, 1, s)
	line(bw, tab(1), `return `, t)
	line(bw, `}`)
	line(bw)
}

type Context struct {
	Rcvr      string
	In        string
	Out       string
	Predicate string
	Depth     int
	Type      *Basic
}

type FieldFace interface {
	Get(*bufio.Writer, Context)
	Set(*bufio.Writer, Context)
}

type DefaultFieldFace struct{}

func (DefaultFieldFace) Get(bw *bufio.Writer, c Context) {
	code(bw, tab(c.Depth), c.Out, `, `, c.Predicate, ` := `)
	line(bw, c.Rcvr, `.Get()`)
}
func (DefaultFieldFace) Set(bw *bufio.Writer, c Context) {
	line(bw, tab(c.Depth), c.Rcvr, `.Set(`, c.In, `)`)
}

type TimeFieldFace struct{}

func (TimeFieldFace) Get(bw *bufio.Writer, c Context) {
	yname := ydbTypeName(c.Type.Primitive)
	line(bw, tab(c.Depth), `var `, c.Out, ` `, c.Type.BaseType.String())
	line(bw, tab(c.Depth), c.Predicate, ` := !`, c.Rcvr, `.IsZero()`)
	line(bw, tab(c.Depth), `if `, c.Predicate, ` {`)
	line(bw, tab(c.Depth+1), c.Out, ` = ydb.Time(`, c.Rcvr, `).`, yname, `()`)
	line(bw, tab(c.Depth), `}`)
}
func (TimeFieldFace) Set(bw *bufio.Writer, c Context) {
	y := ydbTypeName(c.Type.Primitive)
	code(bw, tab(c.Depth), `err := `)
	line(bw, `(*ydb.Time)(&`, c.Rcvr, `).From`, y, `(`, c.In, `)`)
	line(bw, tab(c.Depth), `if err != nil {`)
	line(bw, tab(c.Depth+1), `panic("ydbgen: date types conversion failed: " + err.Error())`)
	line(bw, tab(c.Depth), `}`)
}

func (g *Generator) assignValue(bw *bufio.Writer, depth int, rcvr string, t T) string {
	val := g.declare("v")

	line(bw, tab(depth), `var `, val, ` ydb.Value`)
	line(bw, tab(depth), `{`)
	defer line(bw, tab(depth), `}`)

	g.enterScope()
	defer g.leaveScope()

	var r string
	switch {
	case t.Struct != nil:
		r = g.assignStructValue(bw, depth+1, rcvr, t.Struct)
	case t.Slice != nil:
		r = g.assignSliceValue(bw, depth+1, rcvr, t.Slice)
	default:
		if t.Basic.Face != nil {
			r = g.assignBasicFaceValue(bw, depth+1, rcvr, t)
		} else {
			r = g.assignBasicValue(bw, depth+1, rcvr, t)
		}
	}
	line(bw, tab(depth+1), val, ` = `, r)

	return val
}

func (g *Generator) assignBasicFaceValue(bw *bufio.Writer, depth int, rcvr string, t T) string {
	var (
		x   = g.declare("x")
		ok  = g.declare("ok")
		val = g.declare("v")
	)
	line(bw, tab(depth), `var `, val, ` ydb.Value`)
	t.Basic.Face.Get(bw, Context{
		Rcvr:      rcvr,
		Out:       x,
		Predicate: ok,
		Depth:     depth,
		Type:      t.Basic,
	})
	line(bw, tab(depth), `if `, ok, ` {`)
	code(bw, tab(depth+1), val, ` = `)
	if t.Optional {
		code(bw, `ydb.OptionalValue(`)
	}
	code(bw, `ydb.`, ydbTypeName(t.Basic.Primitive), `Value(`)
	g.convert(bw, t.Basic.Conv, x, t.Basic.Type, t.Basic.BaseType)
	code(bw, `)`)
	if t.Optional {
		code(bw, `)`)
	}
	line(bw)
	line(bw, tab(depth), `} else {`)
	if t.Optional {
		code(bw, tab(depth+1), val, ` = `)
		line(bw, `ydb.NullValue(ydb.Type`, ydbTypeName(t.Basic.Primitive), `)`)
	} else {
		line(bw, tab(depth+1), `panic("ydbgen: no value for non-optional types")`)
	}
	line(bw, tab(depth), `}`)
	return val
}

func (g *Generator) assignBasicValue(bw *bufio.Writer, depth int, x string, t T) string {
	val := g.declare("vp")
	code(bw, tab(depth), val, ` := `)
	if t.Optional {
		code(bw, `ydb.OptionalValue(`)
	}
	code(bw, `ydb.`, ydbTypeName(t.Basic.Primitive), `Value(`)
	g.convert(bw, t.Basic.Conv, x, t.Basic.Type, t.Basic.BaseType)
	if t.Optional {
		code(bw, `)`)
	}
	line(bw, `)`)
	return val
}

func (g *Generator) structFieldWriter(bw *bufio.Writer, depth int, rcvr string, s *Struct) (write func(*Field)) {
	value := map[string]string{}
	for _, f := range s.Fields {
		value[f.Name] = g.assignValue(bw, depth, rcvr+"."+f.Name, f.T)
	}
	return func(f *Field) {
		val, ok := value[f.Name]
		if !ok {
			panic("ydbgen: unexpected field: " + f.Name)
		}
		code(bw, val)
	}
}

func (g *Generator) assignStructValue(bw *bufio.Writer, depth int, rcvr string, s *Struct) string {
	val := g.declare("v")
	line(bw, tab(depth), `var `, val, ` ydb.Value`)
	line(bw, tab(depth), `{`)
	defer line(bw, tab(depth), `}`)

	g.enterScope()
	defer g.leaveScope()

	writeField := g.structFieldWriter(bw, depth+1, rcvr, s)
	line(bw, tab(depth+1), val, ` = ydb.StructValue(`)
	for _, f := range s.Fields {
		code(bw, tab(depth+2))
		code(bw, `ydb.StructFieldValue("`, f.Column, `", `)
		writeField(f)
		line(bw, `),`)
	}
	line(bw, tab(depth+1), `)`)

	return val
}

func (g *Generator) assignStructType(bw *bufio.Writer, depth int, s *Struct) string {
	t := g.declare("t")
	line(bw, tab(depth), `var `, t, ` ydb.Type`)
	line(bw, tab(depth), `{`)
	defer line(bw, tab(depth), `}`)

	g.enterScope()
	defer g.leaveScope()

	fields := g.declare("fs")
	size := strconv.Itoa(len(s.Fields))
	line(bw, tab(depth+1), fields, ` := make([]ydb.StructOption, `, size, `)`)
	for i, f := range s.Fields {
		r := g.assignType(bw, depth+1, f.T)
		id := strconv.Itoa(i)
		code(bw, tab(depth+1), fields, `[`, id, `]`)
		line(bw, ` = ydb.StructField("`, f.Column, `", `, r, `)`)
	}

	line(bw, tab(depth+1), t, ` = ydb.Struct(`, fields, `...)`)

	return t
}

func (g *Generator) assignSliceType(bw *bufio.Writer, depth int, s *Slice) string {
	t := g.declare("t")
	r := g.assignType(bw, depth, s.T)
	line(bw, tab(depth), t, ` := ydb.List(`, r, `)`)
	return t
}

func (g *Generator) assignType(bw *bufio.Writer, depth int, t T) string {
	typ := g.declare("t")

	line(bw, tab(depth), `var `, typ, ` ydb.Type`)
	line(bw, tab(depth), `{`)
	defer line(bw, tab(depth), `}`)

	g.enterScope()
	defer g.leaveScope()

	var r string
	switch {
	case t.Struct != nil:
		r = g.assignStructType(bw, depth+1, t.Struct)
	case t.Slice != nil:
		r = g.assignSliceType(bw, depth+1, t.Slice)
	default:
		r = g.assignBasicType(bw, depth+1, t.Basic)
	}
	code(bw, tab(depth+1), typ, ` = `)
	if t.Optional {
		code(bw, `ydb.Optional(`)
	}
	code(bw, r)
	if t.Optional {
		code(bw, `)`)
	}
	line(bw)

	return typ
}

func (g *Generator) assignBasicType(bw *bufio.Writer, depth int, t *Basic) string {
	tp := g.declare("tp")
	line(bw, tab(depth), tp, ` := ydb.Type`, ydbTypeName(t.Primitive))
	return tp
}

func (g *Generator) writeStructFieldsScan(bw *bufio.Writer, depth int, res, rcvr string, s *Struct) {
	for _, f := range s.Fields {
		// Do not seek to the field name if it is not a container – in such
		// case struct is a flattened tuple.
		flatten := f.T.Struct != nil && !f.T.Container
		if !flatten {
			if s.SeekMode == SeekPosition {
				line(bw, tab(depth), res, `.NextItem()`)
			} else {
				line(bw, tab(depth), res, `.SeekItem("`, f.Column, `")`)
			}
		}
		g.writeScan(bw, depth, res, rcvr+"."+f.Name, f.T)
		if !flatten {
			line(bw)
		}
	}
}

func fieldSetterFunc(s *Struct, column string) string {
	return fmt.Sprintf("ydbgenSet%s%s", s.Name, column)
}

func (g *Generator) writeStructContainerScan(bw *bufio.Writer, depth int, res, rcvr string, s *Struct) {
	g.enterScope()
	defer g.leaveScope()
	var (
		i = g.declare("i")
		n = g.declare("n")
	)
	line(bw, tab(depth), `for `, i, `, `, n, ` := 0, `, res, `.StructIn(); `, i, ` < `, n, `; `, i, `++ {`)
	defer func() {
		line(bw, tab(depth), `}`)
		line(bw, tab(depth), res, `.StructOut()`)
	}()

	line(bw, tab(depth+1), `switch `, res, `.StructField(`, i, `) {`)
	for _, f := range s.Fields {
		line(bw, tab(depth+1), `case "`, f.Column, `":`)
		g.writeScan(bw, depth+2, res, rcvr+"."+f.Name, f.T)
	}
	line(bw, tab(depth+1), `}`)
}

func (g *Generator) writeListContainerScan(bw *bufio.Writer, depth int, res, rcvr string, s *Slice) {
	var (
		n  = g.declare("n")
		xs = g.declare("xs")
	)
	line(bw, tab(depth), n, ` := `, res, `.ListIn()`)
	line(bw, tab(depth), xs, ` := make([]`, goTypeString(s.T), `, `, n, `)`)

	g.enterScope()
	defer g.leaveScope()
	var (
		i = g.declare("i")
		x = g.declare("x")
	)
	line(bw, tab(depth), `for `, i, ` := 0; `, i, ` < `, n, `; `, i, `++ {`)
	defer func() {
		line(bw, tab(depth), `}`)
		line(bw, tab(depth), rcvr, ` = `, xs)
		line(bw, tab(depth), ``, res, `.ListOut()`)
	}()

	line(bw, tab(depth+1), `res.ListItem(`, i, `)`)
	line(bw, tab(depth+1), `var `, x, ` `, goTypeString(s.T))
	g.writeScan(bw, depth+1, "res", x, s.T)
	line(bw, tab(depth+1), xs, `[`, i, `] = `, x)
}

func (g *Generator) writeScan(bw *bufio.Writer, depth int, res, rcvr string, t T) {
	switch {
	case t.Struct != nil:
		if t.Container {
			g.writeStructContainerScan(bw, depth, res, rcvr, t.Struct)
		} else {
			g.writeStructFieldsScan(bw, depth, res, rcvr, t.Struct)
		}

	case t.Slice != nil:
		if t.Container {
			g.writeListContainerScan(bw, depth, res, rcvr, t.Slice)
		}

	default:
		if t.Basic.Face != nil {
			g.writeBasicFaceScan(bw, depth, res, rcvr, t)
		} else {
			g.writeBasicScan(bw, depth, res, rcvr, t)
		}
	}
}

func (g *Generator) writeBasicFaceScan(bw *bufio.Writer, depth int, res, rcvr string, t T) {
	if t.Optional {
		line(bw, tab(depth), res, `.Unwrap()`)
		line(bw, tab(depth), `if !`, res, `.IsNull() {`)
	} else {
		line(bw, tab(depth), `{`)
	}
	defer line(bw, tab(depth), `}`)

	g.enterScope()
	defer g.leaveScope()

	var (
		x = g.declare("x")
	)
	code(bw, tab(depth+1), x, ` := `)
	val := res + "." + ydbTypeName(t.Basic.Primitive) + "()"
	g.convert(bw, t.Basic.Conv, val, t.Basic.BaseType, t.Basic.Type)
	line(bw)
	t.Basic.Face.Set(bw, Context{
		Rcvr:  rcvr,
		Type:  t.Basic,
		In:    x,
		Depth: depth + 1,
	})
}

func (g *Generator) writeBasicScan(bw *bufio.Writer, depth int, res, rcvr string, t T) {
	code(bw, tab(depth), rcvr, " = ")
	val := res + "."
	if t.Optional {
		val += "O"
	}
	val += ydbTypeName(t.Basic.Primitive) + "()"
	g.convert(bw, t.Basic.Conv, val, t.Basic.BaseType, t.Basic.Type)
	line(bw)
}

func sizeof(t types.Type) int {
	b := t.(*types.Basic)
	switch b.Kind() {
	case types.Int, types.Uint:
		return 0
	case types.Int8, types.Uint8:
		return 8
	case types.Int16, types.Uint16:
		return 16
	case types.Int32, types.Uint32:
		return 32
	case types.Int64, types.Uint64:
		return 64
	}
	return -1
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

func typeShortName(t types.Type) string {
	const upper = ^byte('a' - 'A')
	var (
		s   = t.String()
		n   = len(s)
		bts = make([]byte, 1, 3)
	)
	bts[0] = s[0] & upper
	for i := n - 2; i < n; i++ {
		if isDigit(s[i]) {
			bts = append(bts, s[i])
		}
	}
	return string(bts)
}

func (g *Generator) convert(bw *bufio.Writer, mode ConvMode, name string, src, dst types.Type) {
	if src == dst || isByteSlices(src, dst) {
		code(bw, name)
		return
	}
	var conv string
	if mode == ConvAssert && !isSafeConversion(src, dst) {
		conv = g.conversionFunc(src, dst)
	} else {
		conv = dst.String()
	}
	code(bw, conv, `(`, name, `)`)
}

func (g *Generator) conversionFunc(t1, t2 types.Type) string {
	if _, _, bothBasic := basic(t1, t2); !bothBasic {
		return t2.String()
	}
	name := strings.Join([]string{
		"ydbConv",
		typeShortName(t1),
		"To",
		typeShortName(t2),
	}, "")
	if _, ok := g.conversions[name]; !ok {
		g.conversions[name] = ConversionTemplate{
			Name: name,
			Type: [2]types.Type{t1, t2},
		}
	}
	return name
}

type ConversionTemplate struct {
	Name string
	Type [2]types.Type
}

func (t ConversionTemplate) Write(bw *bufio.Writer) {
	var (
		IntInt   = [2]bool{false, false}
		UintUint = [2]bool{true, true}
		IntUint  = [2]bool{false, true}
		UintInt  = [2]bool{true, false}
	)

	code(bw, `func `, t.Name, `(`)
	code(bw, `x `, t.Type[0].String(), `)`)
	line(bw, ` `, t.Type[1].String(), ` {`)
	defer func() {
		line(bw, tab(1), `return `, t.Type[1].String(), `(x)`)
		line(bw, `}`)
		line(bw)
	}()
	b0, b1, ok := basic(t.Type[0], t.Type[1])
	if !ok {
		panic("ydbgen: internal: not a basic types for conversion")
	}
	var (
		i0 = b0.Info()
		i1 = b1.Info()
		s0 = sizeof(b0)
		s1 = sizeof(b1)
	)
	if i0&types.IsInteger == 0 || i1&types.IsInteger == 0 {
		panic("ydbgen: internal: not an integer types for conversion")
	}
	signconv := [2]bool{
		i0&types.IsUnsigned != 0,
		i1&types.IsUnsigned != 0,
	}
	if signconv == IntUint {
		line(bw, tab(1), `if x < 0 {`)
		code(bw, tab(2), `panic("ydbgen: convassert: conversion of negative `)
		line(bw, b0.String(), ` to `, b1.String(), `")`)
		line(bw, tab(1), `}`)
	}
	if s0 <= s1 && signconv != UintInt {
		return
	}

	line(bw, tab(1), `const (`)
	switch b1.Kind() {
	case types.Int, types.Uint:
		line(bw, tab(2), `bits = 32 << (^uint(0) >> 63)`)
	default:
		line(bw, tab(2), `bits = `, strconv.Itoa(s1))
	}
	code(bw, tab(2), `mask = (1 << (bits`)
	if signconv == UintInt || signconv == IntInt {
		code(bw, ` - 1))`)
	} else {
		code(bw, `))`)
	}
	line(bw, ` - 1`)
	line(bw, tab(1), `)`)

	switch signconv {
	case IntInt, IntUint:
		abs(bw, 1, "x", "abs")
	case UintInt, UintUint:
		line(bw, tab(1), `abs := uint64(x)`)
	}
	line(bw, tab(1), `if abs&mask != abs {`)
	line(bw, tab(2), `panic(`)
	code(bw, tab(3), `"ydbgen: convassert: " + `)
	format(bw, i0, 2, "x")
	line(bw, ` +`)
	code(bw, tab(4), `" (types `, b0.String(), `)`)
	line(bw, ` overflows `, b1.String(), `",`)
	line(bw, tab(2), `)`)
	line(bw, tab(1), `}`)
}

func abs(bw *bufio.Writer, depth int, in, out string) {
	line(bw, tab(depth), `var `, out, ` uint64`)
	line(bw, tab(depth), `{`)
	line(bw, tab(depth+1), `v := int64(`, in, `)`)
	line(bw, tab(depth+1), `m := v >> 63`)
	line(bw, tab(depth+1), out, ` = uint64(v ^ m - m)`)
	line(bw, tab(depth), `}`)
}

func format(bw *bufio.Writer, info types.BasicInfo, depth int, in string) {
	var (
		t string
		w string
	)
	if info&types.IsUnsigned != 0 {
		t = "Uint"
		w = "uint64"
	} else {
		t = "Int"
		w = "int64"
	}
	code(bw, `strconv.Format`)
	code(bw, t, `(`, w, `(`, in, `), 10)`)
}

func line(bw *bufio.Writer, args ...string) {
	code(bw, args...)
	_ = bw.WriteByte('\n')
}

func code(bw *bufio.Writer, args ...string) {
	for _, arg := range args {
		_, _ = bw.WriteString(arg)
	}
}

func ydbTypeName(t internal.PrimitiveType) string {
	switch t {
	case internal.TypeBool:
	case internal.TypeInt8:
	case internal.TypeUint8:
	case internal.TypeInt16:
	case internal.TypeUint16:
	case internal.TypeInt32:
	case internal.TypeUint32:
	case internal.TypeInt64:
	case internal.TypeUint64:
	case internal.TypeFloat:
	case internal.TypeDouble:
	case internal.TypeDate:
	case internal.TypeDatetime:
	case internal.TypeTimestamp:
	case internal.TypeInterval:
	case internal.TypeTzDate:
	case internal.TypeTzDatetime:
	case internal.TypeTzTimestamp:
	case internal.TypeString:

	case internal.TypeUTF8:
		return "UTF8"
	case internal.TypeYSON:
		return "YSON"
	case internal.TypeJSON:
		return "JSON"
	case internal.TypeUUID:
		return "UUID"
	case internal.TypeJSONDocument:
		return "JSONDocument"
	case internal.TypeDyNumber:
		return "DyNumber"

	default:
		panic("ydbgen: unexpected primitive types")
	}
	return t.String()
}

func goTypeString(t T) string {
	switch {
	case t.Slice != nil:
		return "[]" + goTypeString(t.Slice.T)
	case t.Struct != nil:
		return t.Struct.Name
	default:
		return typeString(t.Basic.Type)
	}
}

func typeString(t types.Type) string {
	return types.TypeString(t, func(p *types.Package) string {
		return ""
	})
}
