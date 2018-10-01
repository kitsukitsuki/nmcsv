import
  lexbase as lb,
  streams as streams

type
  ParserState = enum
    beforeField, startField, escapedChar, inField, afterField
    inQuotedField, escapeInQuotedField, quoteInQuotedField,
    afterEscapedCrnl, endParser

  CsvRow = seq[string]
  CsvLexer = object of lb.BaseLexer
  CsvParams = object
    delimiter, quotechar, escapechar: char
    skipInitSpace: bool
  CsvReader* = object  ## csv reader object
    row*: CsvRow
    lexer: CsvLexer
    params: CsvParams
    pathFile: string
    maxLen: int
    state: ParserState

  CsvError = object of IOError




# Forward declarations
proc reader*(cr: var CsvReader, fileStream: streams.Stream,
            delimiter=',', quotechar='"', escapechar='\0',
            skipInitialSpace=false)
proc readRow*(cr: var CsvReader, columns=0): bool

proc parseField(s: var string, pos: var int, lexer: var CsvLexer,
                state: var ParserState, params: CsvParams)
proc close*(cr: var CsvReader)
proc error(cr: CsvReader, msg: string)
proc raiseInvalidCsvError(msg: string)

###
#  reader
###
proc reader*(cr: var CsvReader, fileStream: streams.Stream,
            delimiter=',', quotechar='"', escapechar='\0',
            skipInitialSpace=false) =
  lb.open(cr.lexer, fileStream)

  cr.params.delimiter = delimiter
  cr.params.quotechar = quotechar
  cr.params.escapechar = escapechar
  cr.params.skipInitSpace = skipInitialSpace
  cr.row = @[]
  cr.maxLen = 0

  cr.state = beforeField

proc parseRow(row: var CsvRow, state: var ParserState,
              params: CsvParams, lexer: var CsvLexer): bool =
  var
    buf = lexer.buf
    pos = lexer.bufpos
    col = 0
    maxLen = 0

  setLen(row, maxLen)
  while state != endParser:
    if maxLen < col+1:  # This row is longest; update len of row and maxLen
      maxLen = col + 1
      setLen(row, maxLen)

    parseField(row[col], lexer.bufpos, lexer, state, params)
    pos = lexer.bufpos
    inc(col)

  setLen(row, col)
  state = beforeField
  result = (nil notin row) and (col > 0)

proc readRow*(cr: var CsvReader, columns=0): bool {.discardable.} =
  result = parseRow(cr.row, cr.state, cr.params, cr.lexer)

proc next*(cr: var CsvReader, columns=0): CsvRow =
  if readRow(cr):
    result = cr.row
  else:
    result = @[]

proc toSeq*(cr: var CsvReader): seq[CsvRow] =
  result = @[]
  while readRow(cr):
    result.add(cr.row)

proc handleCrlf(lx: var CsvLexer, pos: var int, c: char) =
  case c:
    of '\c':
      pos = lb.handleCR(lx, pos)
    of '\l':
      pos = lb.handleLF(lx, pos)
    else:
      # error
      discard


proc parseField(s: var string, pos: var int, lexer: var CsvLexer,
                state: var ParserState, params: CsvParams) =
  var
    buf = lexer.buf
  let
    quote = params.quotechar
    esc = params.escapechar
    delim = params.delimiter
    skip = params.skipInitSpace

  # init string or reuse memory
  if s.isNil:
    s = newString(0)
  else:
    setLen(s, 0)

  while true:
    let
      c = buf[pos]

    case state:
      of beforeField:
        # before parsing field
        if c == '\0':
          # empty line; return nil and end parser
          s = nil
          inc(pos)
          state = endParser
          break
        elif c in {'\c', '\l'}:
          # new line; handle CR and LF and end parser
          handleCrlf(lexer, pos, c)
          state = endParser
          break
        elif c == delim:
          state = afterField
        elif c == quote:
          # quotechar; inc pos and get in quoted field
          inc(pos)
          state = inQuotedField
        elif c == ' ':
          # blank; skip or get in field
          if skip:
            inc(pos)
          else:
            state = inField
        else:
          # normal character; get in field
          state = inField
      of inQuotedField:
        # quoted field
        if c == '\0' and esc != '\0':
          # end of line and end parser
          inc(pos)
          state = endParser
          break
        elif c == esc:
          add(s, c)
          inc(pos)
        elif c in {'\c', '\l'}:
          handleCrlf(lexer, pos, c)
          add(s, "\n")
        elif c == quote:
          # quote character
          if buf[pos+1] ==  delim or buf[pos+1] in {'\0', '\c', '\l'}:
            inc(pos)
            state = afterField
          else:
            add(s, c)
            inc(pos)
        else:
          # normal character
          add(s, c)
          inc(pos)
      of inField:
        if c == '\0':
          # end of line and end parser
          inc(pos)
          state = endParser
          break
        elif c == esc:
          add(s, c)
          inc(pos)
        elif c in {'\c', '\l'}:
          handleCrlf(lexer, pos, c)
          state = endParser
          break
        elif c == delim:
          state = afterField
        else:
          # normal character
          add(s, c)
          inc(pos)
      of afterField:
        if c == '\0':
          inc(pos)
          state = endParser
          break
        elif c in {'\c', '\l'}:
          handleCrlf(lexer, pos, c)
          state = endParser
          break
        elif c == delim:
          # delimiter; go to next field
          inc(pos)
          state = beforeField
          break
        else:
          state = beforeField
          # break
      else:
        discard

proc close*(cr: var CsvReader) =
  lb.close(cr.lexer)

proc error(cr: CsvReader, msg: string) =
  raiseInvalidCsvError(msg)

proc raiseInvalidCsvError(msg: string) =
  var e: ref CsvError
  new(e)
  e.msg = "Error:" & msg
  raise e


when isMainModule:
  let pathFile = "test.csv"
  var seq2dCsv: seq[seq[string]] = @[]
  var debugType = 2

  case debugtype:
    of 1:
      block:
        var fs = newFileStream(pathFile, fmRead)
        if not isNil(fs):
          var cr: CsvReader
          cr.reader(fs)
          while readRow(cr):
            seq2dCsv.add(cr.row)
          defer: cr.close()
        else:
          echo "file is nil"
    of 2:
      block:
        var fs = newFileStream(pathFile, fmRead)
        if not isNil(fs):
          var cr: CsvReader
          cr.reader(fs)
          seq2dCsv = cr.toSeq()
          defer: cr.close()
        else:
          echo "file is nil"
    else:
      discard

  # echo seq2dCsv
