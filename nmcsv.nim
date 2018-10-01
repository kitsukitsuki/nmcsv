import
  lexbase as lb,
  streams as streams

type
  ParserState = enum
    beforeField, startField, escapedChar, inField, afterField
    inQuotedField, escapeInQuotedField, quoteInQuotedField,
    afterEscapedCrnl, endParser

  CsvRow = seq[string]
  CsvReader* = object of lb.BaseLexer  ## csv reader object
    row*: CsvRow
    pathFile: string
    delimiter, quotechar, escapechar: char
    skipInitSpace: bool
    maxLen: int
    state: ParserState

  CsvError = object of IOError




# Forward declarations
proc reader*(cr: var CsvReader, fileStream: streams.Stream,
            delimiter=',', quotechar='"', escapechar='\0',
            skipInitialSpace=false)
proc readRow*(cr: var CsvReader, columns=0): CsvRow
proc parseField(cr: var CsvReader, s: var string, pos: var int,
                state: var ParserState)
proc close*(cr: var CsvReader)
proc error(cr: CsvReader, msg: string)
proc raiseInvalidCsvError(msg: string)

###
#  reader
###
proc reader*(cr: var CsvReader, fileStream: streams.Stream,
            delimiter=',', quotechar='"', escapechar='\0',
            skipInitialSpace=false) =
  lb.open(cr, fileStream)

  cr.delimiter = delimiter
  cr.quotechar = quotechar
  cr.escapechar = escapechar
  cr.skipInitSpace = skipInitialSpace
  cr.row = @[]
  cr.maxLen = 0

  cr.state = beforeField

proc next*(cr: var CsvReader, columns=0): bool {.discardable.} =
  var
    state = cr.state
    buf = cr.buf
    pos = cr.bufpos
    col = 0
  let
    maxLen = cr.maxLen
    oldpos = cr.bufpos
    esc = cr.escapechar
    delim = cr.delimiter

  setLen(cr.row, maxLen)
  while cr.state != endParser:
    if maxLen < col+1:  # This row is longest; update len of row and maxLen
      setLen(cr.row, col+1)
      cr.maxLen = col+1

    parseField(cr, cr.row[col], cr.bufpos, cr.state)
    pos = cr.bufpos
    inc(col)

  setLen(cr.row, col)
  cr.state = beforeField
  result = (nil notin cr.row) and (col > 0)

  if result and col != columns and columns > 0:
    error(cr, "error")

proc readRow*(cr: var CsvReader, columns=0): CsvRow =
  if next(cr):
    result = cr.row
  else:
    result = @[]

proc readRows*(cr: var CsvReader): seq[CsvRow] =
  result = @[]
  while next(cr):
    result.add(cr.row)



proc handleCrlf(cr: var CsvReader, pos: var int, c: char) =
  case c:
    of '\c':
      pos = lb.handleCR(cr, pos)
    of '\l':
      pos = lb.handleLF(cr, pos)
    else:
      # error
      discard


proc parseField(cr: var CsvReader, s: var string, pos: var int,
                state: var ParserState) =
  var
    buf = cr.buf
  let
    quote = cr.quotechar
    esc = cr.escapechar
    delim = cr.delimiter

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
          handleCrlf(cr, pos, c)
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
          if cr.skipInitSpace:
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
          handleCrlf(cr, pos, c)
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
          handleCrlf(cr, pos, c)
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
          handleCrlf(cr, pos, c)
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
  lb.close(cr)

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
  var debugType = 1

  case debugtype:
    of 1:
      block:
        var fs = newFileStream(pathFile, fmRead)
        if not isNil(fs):
          var cr: CsvReader
          cr.reader(fs)
          seq2dCsv = cr.readRows
          defer: cr.close()
        else:
          echo "file is nil"
    else:
      discard


  # echo seq2dCsv
