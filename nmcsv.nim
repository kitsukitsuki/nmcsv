import
  lexbase as lb,
  streams as streams

type
  CsvRow = seq[string]
  CsvReader* = object of lb.BaseLexer  ## csv reader object
    row*: CsvRow
    pathFile: string
    delimiter, quotechar, escapechar: char
    skipInitSpace: bool

  CsvError = object of IOError

# Forward declarations
proc reader*(cr: var CsvReader, fileStream: streams.Stream,
            delimiter=',', quotechar='"', escapechar='\0',
            skipInitialSpace=false)
proc readRow*(cr: var CsvReader, columns=0): bool
proc parseField(cr: var CsvReader, s: var string)
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

proc readRow*(cr: var CsvReader, columns=0): bool =
  var
    buf = cr.buf
    pos = cr.bufpos
    col = 0
  let
    oldpos = cr.bufpos
    esc = cr.escapechar
    delim = cr.delimiter

  while buf[pos] != '\0':
    let oldlen = cr.row.len
    if oldlen < col+1:
      setLen(cr.row, col+1)
      cr.row[col] = ""
    parseField(cr, cr.row[col])
    pos = cr.bufpos
    inc(col)
    if buf[pos] == delim:
      inc(cr.bufpos)
    else:
      case buf[pos]
      of '\c', '\l':
        while true:
          case buf[cr.bufpos]
          of '\c':
            cr.bufpos = lb.handleCR(cr, pos)
          of '\l':
            cr.bufpos = lb.handleLF(cr, pos)
          else:
            break
      of '\0':
        discard
      else:
        #Error
        error(cr, delim & " error")
      break
  setLen(cr.row, col)
  result = col > 0
  if result and col != columns and columns > 0:
    error(cr, "error")


proc parseField(cr: var CsvReader, s: var string) =
  var
    buf = cr.buf
    pos = cr.bufpos
  let
    quote = cr.quotechar
    esc = cr.escapechar
    delim = cr.delimiter

  if cr.skipInitSpace:
    while buf[pos] in {' ', '\t'}:
      inc(pos)

  setLen(s, 0)
  if buf[pos] == quote and quote != '\0':
    inc(pos)
    while true:
      let c = buf[pos]
      if c == '\0':
        cr.bufpos = pos
        # Error
        break
      elif c == quote:
        if esc == '\0' and buf[pos+1] == quote:
          add(s, quote)
          inc(pos, 2)
        else:
          inc(pos)
          break
      elif c == esc:
        add(s, buf[pos+1])
        inc(pos, 2)
      else:
        case c:
          of '\c':
            pos = lb.handleCR(cr, pos)
            buf = cr.buf
            add(s, "\n")
          of '\l':
            pos = lb.handleLF(cr, pos)
            buf = cr.buf
            add(s, "\n")
          else:
            add(s, c)
            inc(pos)
  else:
    while true:
      let c = buf[pos]
      if c == delim: break
      if c in {'\c', '\l', '\0'}: break
      add(s, c)
      inc(pos)
  cr.bufpos = pos

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
  var seq2dCsv: seq[seq[string]] = @[]
  block:
    var fs = newFileStream("tmp.csv", fmRead)
    if not isNil(fs):
      var cr: CsvReader
      cr.reader(fs)
      echo cr.readRow()
      echo cr.row
      while cr.readRow():
        echo cr.row
        seq2dCsv &= cr.row
      defer: cr.close()
    else:
      echo "file is nil"

    echo seq2dCsv
