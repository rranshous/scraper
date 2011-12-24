#
# Autogenerated by Thrift
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

from thrift.Thrift import *
from ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class Iface:
  def get_links(self, url):
    """
    Parameters:
     - url
    """
    pass

  def get_images(self, url):
    """
    Parameters:
     - url
    """
    pass

  def link_spider(self, root_url, max_depth):
    """
    Parameters:
     - root_url
     - max_depth
    """
    pass


class Client(Iface):
  def __init__(self, iprot, oprot=None):
    self._iprot = self._oprot = iprot
    if oprot != None:
      self._oprot = oprot
    self._seqid = 0

  def get_links(self, url):
    """
    Parameters:
     - url
    """
    self.send_get_links(url)
    return self.recv_get_links()

  def send_get_links(self, url):
    self._oprot.writeMessageBegin('get_links', TMessageType.CALL, self._seqid)
    args = get_links_args()
    args.url = url
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_get_links(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = get_links_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success != None:
      return result.success
    if result.ex != None:
      raise result.ex
    raise TApplicationException(TApplicationException.MISSING_RESULT, "get_links failed: unknown result");

  def get_images(self, url):
    """
    Parameters:
     - url
    """
    self.send_get_images(url)
    return self.recv_get_images()

  def send_get_images(self, url):
    self._oprot.writeMessageBegin('get_images', TMessageType.CALL, self._seqid)
    args = get_images_args()
    args.url = url
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_get_images(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = get_images_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success != None:
      return result.success
    if result.ex != None:
      raise result.ex
    raise TApplicationException(TApplicationException.MISSING_RESULT, "get_images failed: unknown result");

  def link_spider(self, root_url, max_depth):
    """
    Parameters:
     - root_url
     - max_depth
    """
    self.send_link_spider(root_url, max_depth)
    return self.recv_link_spider()

  def send_link_spider(self, root_url, max_depth):
    self._oprot.writeMessageBegin('link_spider', TMessageType.CALL, self._seqid)
    args = link_spider_args()
    args.root_url = root_url
    args.max_depth = max_depth
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_link_spider(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = link_spider_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success != None:
      return result.success
    if result.ex != None:
      raise result.ex
    raise TApplicationException(TApplicationException.MISSING_RESULT, "link_spider failed: unknown result");


class Processor(Iface, TProcessor):
  def __init__(self, handler):
    self._handler = handler
    self._processMap = {}
    self._processMap["get_links"] = Processor.process_get_links
    self._processMap["get_images"] = Processor.process_get_images
    self._processMap["link_spider"] = Processor.process_link_spider

  def process(self, iprot, oprot):
    (name, type, seqid) = iprot.readMessageBegin()
    if name not in self._processMap:
      iprot.skip(TType.STRUCT)
      iprot.readMessageEnd()
      x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
      oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.trans.flush()
      return
    else:
      self._processMap[name](self, seqid, iprot, oprot)
    return True

  def process_get_links(self, seqid, iprot, oprot):
    args = get_links_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = get_links_result()
    try:
      result.success = self._handler.get_links(args.url)
    except Exception, ex:
      result.ex = ex
    oprot.writeMessageBegin("get_links", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_get_images(self, seqid, iprot, oprot):
    args = get_images_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = get_images_result()
    try:
      result.success = self._handler.get_images(args.url)
    except Exception, ex:
      result.ex = ex
    oprot.writeMessageBegin("get_images", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_link_spider(self, seqid, iprot, oprot):
    args = link_spider_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = link_spider_result()
    try:
      result.success = self._handler.link_spider(args.root_url, args.max_depth)
    except Exception, ex:
      result.ex = ex
    oprot.writeMessageBegin("link_spider", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()


# HELPER FUNCTIONS AND STRUCTURES

class get_links_args:
  """
  Attributes:
   - url
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'url', None, None, ), # 1
  )

  def __init__(self, url=None,):
    self.url = url

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.url = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('get_links_args')
    if self.url != None:
      oprot.writeFieldBegin('url', TType.STRING, 1)
      oprot.writeString(self.url)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class get_links_result:
  """
  Attributes:
   - success
   - ex
  """

  thrift_spec = (
    (0, TType.LIST, 'success', (TType.STRING,None), None, ), # 0
    (1, TType.STRUCT, 'ex', (Exception, Exception.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, ex=None,):
    self.success = success
    self.ex = ex

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.LIST:
          self.success = []
          (_etype3, _size0) = iprot.readListBegin()
          for _i4 in xrange(_size0):
            _elem5 = iprot.readString();
            self.success.append(_elem5)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.ex = Exception()
          self.ex.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('get_links_result')
    if self.success != None:
      oprot.writeFieldBegin('success', TType.LIST, 0)
      oprot.writeListBegin(TType.STRING, len(self.success))
      for iter6 in self.success:
        oprot.writeString(iter6)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.ex != None:
      oprot.writeFieldBegin('ex', TType.STRUCT, 1)
      self.ex.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class get_images_args:
  """
  Attributes:
   - url
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'url', None, None, ), # 1
  )

  def __init__(self, url=None,):
    self.url = url

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.url = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('get_images_args')
    if self.url != None:
      oprot.writeFieldBegin('url', TType.STRING, 1)
      oprot.writeString(self.url)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class get_images_result:
  """
  Attributes:
   - success
   - ex
  """

  thrift_spec = (
    (0, TType.LIST, 'success', (TType.STRING,None), None, ), # 0
    (1, TType.STRUCT, 'ex', (Exception, Exception.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, ex=None,):
    self.success = success
    self.ex = ex

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.LIST:
          self.success = []
          (_etype10, _size7) = iprot.readListBegin()
          for _i11 in xrange(_size7):
            _elem12 = iprot.readString();
            self.success.append(_elem12)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.ex = Exception()
          self.ex.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('get_images_result')
    if self.success != None:
      oprot.writeFieldBegin('success', TType.LIST, 0)
      oprot.writeListBegin(TType.STRING, len(self.success))
      for iter13 in self.success:
        oprot.writeString(iter13)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.ex != None:
      oprot.writeFieldBegin('ex', TType.STRUCT, 1)
      self.ex.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class link_spider_args:
  """
  Attributes:
   - root_url
   - max_depth
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'root_url', None, None, ), # 1
    (2, TType.I32, 'max_depth', None, None, ), # 2
  )

  def __init__(self, root_url=None, max_depth=None,):
    self.root_url = root_url
    self.max_depth = max_depth

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.root_url = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.I32:
          self.max_depth = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('link_spider_args')
    if self.root_url != None:
      oprot.writeFieldBegin('root_url', TType.STRING, 1)
      oprot.writeString(self.root_url)
      oprot.writeFieldEnd()
    if self.max_depth != None:
      oprot.writeFieldBegin('max_depth', TType.I32, 2)
      oprot.writeI32(self.max_depth)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class link_spider_result:
  """
  Attributes:
   - success
   - ex
  """

  thrift_spec = (
    (0, TType.LIST, 'success', (TType.STRING,None), None, ), # 0
    (1, TType.STRUCT, 'ex', (Exception, Exception.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, ex=None,):
    self.success = success
    self.ex = ex

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.LIST:
          self.success = []
          (_etype17, _size14) = iprot.readListBegin()
          for _i18 in xrange(_size14):
            _elem19 = iprot.readString();
            self.success.append(_elem19)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.ex = Exception()
          self.ex.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('link_spider_result')
    if self.success != None:
      oprot.writeFieldBegin('success', TType.LIST, 0)
      oprot.writeListBegin(TType.STRING, len(self.success))
      for iter20 in self.success:
        oprot.writeString(iter20)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.ex != None:
      oprot.writeFieldBegin('ex', TType.STRUCT, 1)
      self.ex.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()
    def validate(self):
      return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)