import
  lexbase as lb,
  streams as streams,
  strutils as strutils,
  os as os

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
    col: int
    pathFile: string
    maxLen: int
    state: ParserState

  CsvError = object of IOError




# Forward declarations
proc reader*(cr: var CsvReader, fileStream: streams.Stream,
            delimiter=',', quotechar='"', escapechar='\0',
            skipInitialSpace=false, bufLen=8192)
proc readRow*(cr: var CsvReader): bool

proc parseField(s: var string, lexer: var CsvLexer,
                state: var ParserState, params: CsvParams) {.inline.}
proc close*(cr: var CsvReader)
proc error(cr: CsvReader, msg: string)
proc raiseInvalidCsvError(msg: string)

###
#  reader
###
proc reader*(cr: var CsvReader, fileStream: streams.Stream,
            delimiter=',', quotechar='"', escapechar='\0',
            skipInitialSpace=false, bufLen=8192) =
  lb.open(cr.lexer, fileStream, bufLen)

  cr.params.delimiter = delimiter
  cr.params.quotechar = quotechar
  cr.params.escapechar = escapechar
  cr.params.skipInitSpace = skipInitialSpace
  cr.row = @[]
  cr.maxLen = 0

  cr.state = beforeField

proc parseRow(row: var CsvRow, col: var int, state: var ParserState,
              params: CsvParams, maxLen: var int,
              lexer: var CsvLexer) {.inline.} =
  var
    buf = lexer.buf
    pos = lexer.bufpos

  col = 0
  setLen(row, maxLen)
  while state != endParser:
    if maxLen < col+1:  # This row is longest; update len of row and maxLen
      maxLen = col + 1
      setLen(row, maxLen)

    parseField(row[col], lexer, state, params)
    pos = lexer.bufpos
    inc(col)

  setLen(row, col)
  state = beforeField

  lexer.bufpos = pos

proc readRow*(cr: var CsvReader): bool {.discardable.} =
  parseRow(cr.row, cr.col, cr.state, cr.params, cr.maxLen, cr.lexer)

  result = (nil notin cr.row) and (cr.col > 0)

proc next*(cr: var CsvReader, columns=0): CsvRow =
  if readRow(cr):
    result = cr.row
  else:
    result = @[]

proc toSeq*(pathFile: string): seq[CsvRow] =
  result = @[]

  # echo os.getFileSize(pathFile)
  var fs = newFileStream(pathFile, fmRead)
  if not isNil(fs):
    var cr: CsvReader
    let bufLen = 8192# int(os.getFileSize(pathFile))
    cr.reader(fs, bufLen=bufLen)

    var
      i = 0
      line = strutils.countLines($cr.lexer.buf)
    setLen(result, line-1)
    while true:
      parseRow(cr.row, cr.col, cr.state, cr.params, cr.maxLen, cr.lexer)
      if (nil notin cr.row) and (cr.col > 0):
        result[i] = cr.row

        inc(i)
        if line <= i + 1:
          line += strutils.countLines($cr.lexer.buf)
          setLen(result, line-1)
      else:
        break
    setLen(result, i)
    cr.close()

  else:
    echo "file is nil"

proc handleCrlf(lx: var CsvLexer, pos: var int, c: char) =
  case c:
    of '\c':
      pos = lb.handleCR(lx, pos)
    of '\l':
      pos = lb.handleLF(lx, pos)
    else:
      # error
      discard


proc parseField(s: var string, lexer: var CsvLexer,
                state: var ParserState, params: CsvParams) {.inline.} =
  var
    buf = lexer.buf
    pos = lexer.bufpos
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
  lexer.bufpos = pos

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
      seq2dCsv = toSeq(pathFile)
    else:
      discard

  # echo seq2dCsv
