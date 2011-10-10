import ctypes
import os
import struct

# XXX 32bit path
_ddscore_lib = ctypes.CDLL(os.path.join(os.environ['NDDSHOME'], 'lib', 'x64Linux2.6gcc4.1.1', 'libnddscore.so'), ctypes.RTLD_GLOBAL)
_ddsc_lib = ctypes.CDLL(os.path.join(os.environ['NDDSHOME'], 'lib', 'x64Linux2.6gcc4.1.1', 'libnddsc.so'))

# Error checkers

class Error(Exception):
    pass

def check_code(result, func, arguments):
    if result != 0:
        raise Error({
            1: 'error',
            2: 'unsupported',
            3: 'bad parameter',
            4: 'precondition not met',
            5: 'out of resources',
            6: 'not enabled',
            7: 'immutable policy',
            8: 'inconsistant policy',
            9: 'already deleted',
            10: 'timeout',
            11: 'no data',
            12: 'illegal operation',
        }[result])

def check_null(result, func, arguments):
    if not result:
        raise Error()
    return result

def check_ex(result, func, arguments):
    if arguments[-1]._obj.value != 0:
        raise Error({
            1: '(user)',
            2: '(system)',
            3: 'bad param (system)',
            4: 'no memory (system)',
            5: 'bad typecode (system)',
            6: 'badkind (user)',
            7: 'bounds (user)',
            8: 'immutable typecode (system)',
            9: 'bad member name (user)',
            10: 'bad member id (user)',
        }[arguments[-1]._obj.value])
    return result

# Function and structure accessors

def get(name, type):
    return ctypes.cast(getattr(_ddsc_lib, 'DDS_' + name), ctypes.POINTER(type)).contents

@apply
class DDSFunc(object):
    def __getattr__(self, attr):
        contents = getattr(_ddsc_lib, 'DDS_' + attr)
        setattr(self, attr, contents)
        return contents

@apply
class DDSVoidP(object):
    def __getattr__(self, attr):
        contents = ctypes.cast(getattr(_ddsc_lib, 'DDS_' + attr), ctypes.c_void_p)
        setattr(self, attr, contents)
        return contents

@apply
class DDSType(object):
    def __getattr__(self, attr):
        contents = type(attr, (ctypes.Structure,), {})
        
        def g(self2, attr2):
            f = getattr(DDSFunc, attr + '_' + attr2)
            def m(*args):
                return f(self2, *args)
            setattr(self2, attr2, m)
            return m
        # make structs dynamically present bound methods
        contents.__getattr__ = g
        # take advantage of POINTERs being cached to make type pointers do the same
        ctypes.POINTER(contents).__getattr__ = g
        
        setattr(self, attr, contents)
        return contents

DDSType.Topic._fields_ = [
    ('_as_Entity', ctypes.c_void_p),
    ('_as_TopicDescription', ctypes.POINTER(DDSType.TopicDescription)),
]
ctypes.POINTER(DDSType.Topic).as_topicdescription = lambda self: self.contents._as_TopicDescription

DDSType.DynamicDataSeq._fields_ = DDSType.SampleInfoSeq._fields_ = [
    ('_owned', ctypes.c_bool),
    ('_contiguous_buffer', ctypes.c_void_p),
    ('_discontiguous_buffer', ctypes.c_void_p),
    ('_maximum', ctypes.c_ulong),
    ('_length', ctypes.c_ulong),
    ('_sequence_init', ctypes.c_long),
    ('_read_token1', ctypes.c_void_p),
    ('_read_token2', ctypes.c_void_p),
    ('_elementPointersAllocation', ctypes.c_bool),
]

# some types
enum = ctypes.c_int

DDS_Char = ctypes.c_char
DDS_Wchar = ctypes.c_wchar
DDS_Octet = ctypes.c_ubyte
DDS_Short = ctypes.c_int16
DDS_UnsignedShort = ctypes.c_uint16
DDS_Long = ctypes.c_int32
DDS_UnsignedLong = ctypes.c_uint32
DDS_LongLong = ctypes.c_int64
DDS_UnsignedLongLong = ctypes.c_uint64
DDS_Float = ctypes.c_float
DDS_Double = ctypes.c_double
DDS_LongDouble = ctypes.c_longdouble
DDS_Boolean = ctypes.c_bool
DDS_Enum = DDS_UnsignedLong

DDS_DynamicDataMemberId = DDS_Long
DDS_ReturnCode_t = DDS_Enum
DDS_ExceptionCode_t = DDS_Enum
def ex():
    return ctypes.byref(DDS_ExceptionCode_t())

DDS_SampleStateMask = DDS_UnsignedLong
DDS_ViewStateMask = DDS_UnsignedLong
DDS_InstanceStateMask = DDS_UnsignedLong

DDS_DYNAMIC_DATA_MEMBER_ID_UNSPECIFIED = 0

# Function prototypes

_dyn_types = dict(
    long=DDS_Long,
    ulong=DDS_UnsignedLong,
    short=DDS_Short,
    ushort=DDS_UnsignedShort,
    longlong=DDS_LongLong,
    ulonglong=DDS_UnsignedLongLong,
    float=DDS_Float,
    double=DDS_Double,
    boolean=DDS_Boolean,
    octet=DDS_Octet,
    char=DDS_Char,
    wchar=DDS_Wchar,
)
map(lambda (p, errcheck, restype, argtypes): (setattr(p, 'errcheck', errcheck) if errcheck is not None else None, setattr(p, 'restype', restype), setattr(p, 'argtypes', argtypes)), [
    (DDSFunc.DomainParticipantFactory_get_instance, check_null, ctypes.POINTER(DDSType.DomainParticipantFactory), []),
    (DDSFunc.DomainParticipantFactory_create_participant, check_null, ctypes.POINTER(DDSType.DomainParticipant), [ctypes.POINTER(DDSType.DomainParticipantFactory), ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong]),
    (DDSFunc.DomainParticipantFactory_delete_participant, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DomainParticipantFactory), ctypes.POINTER(DDSType.DomainParticipant)]),
    
    (DDSFunc.DomainParticipant_create_publisher, check_null, ctypes.POINTER(DDSType.Publisher), [ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong]),
    (DDSFunc.DomainParticipant_delete_publisher, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.Publisher)]),
    (DDSFunc.DomainParticipant_create_subscriber, check_null, ctypes.POINTER(DDSType.Subscriber), [ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong]),
    (DDSFunc.DomainParticipant_delete_subscriber, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.Subscriber)]),
    (DDSFunc.DomainParticipant_create_topic, check_null, ctypes.POINTER(DDSType.Topic), [ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong]),
    (DDSFunc.DomainParticipant_delete_topic, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.Topic)]),
    
    (DDSFunc.Publisher_create_datawriter, check_null, ctypes.POINTER(DDSType.DataWriter), [ctypes.POINTER(DDSType.Publisher), ctypes.POINTER(DDSType.Topic), ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong]),
    (DDSFunc.Publisher_delete_datawriter, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.Publisher), ctypes.POINTER(DDSType.DataWriter)]),
    
    (DDSFunc.Subscriber_create_datareader, check_null, ctypes.POINTER(DDSType.DataReader), [ctypes.POINTER(DDSType.Subscriber), ctypes.POINTER(DDSType.TopicDescription), ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong]),
    (DDSFunc.Subscriber_delete_datareader, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.Subscriber), ctypes.POINTER(DDSType.DataReader)]),
    
    (DDSFunc.DynamicDataTypeSupport_new, check_null, ctypes.POINTER(DDSType.DynamicDataTypeSupport), [ctypes.POINTER(DDSType.TypeCode), ctypes.c_void_p]),
    (DDSFunc.DynamicDataTypeSupport_delete, None, None, [ctypes.POINTER(DDSType.DynamicDataTypeSupport)]),
    (DDSFunc.DynamicDataTypeSupport_register_type, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataTypeSupport), ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p]),
    (DDSFunc.DynamicDataTypeSupport_unregister_type, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataTypeSupport), ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p]),
    (DDSFunc.DynamicDataTypeSupport_create_data, check_null, ctypes.POINTER(DDSType.DynamicData), [ctypes.POINTER(DDSType.DynamicDataTypeSupport)]),
    (DDSFunc.DynamicDataTypeSupport_delete_data, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataTypeSupport), ctypes.POINTER(DDSType.DynamicData)]),
    (DDSFunc.DynamicDataTypeSupport_print_data, None, None, [ctypes.POINTER(DDSType.DynamicDataTypeSupport), ctypes.POINTER(DDSType.DynamicData)]),
    
    (DDSFunc.DynamicData_new, check_null, ctypes.POINTER(DDSType.DynamicData), [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDSType.DynamicDataProperty)]),
] + [
    (getattr(DDSFunc, "DynamicData_get_" + k), check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(v), ctypes.c_char_p, DDS_DynamicDataMemberId])
        for k, v in _dyn_types.iteritems()
] + [
    (getattr(DDSFunc, "DynamicData_set_" + k), check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.c_char_p, DDS_DynamicDataMemberId, v])
        for k, v in _dyn_types.iteritems()
] + [
    (DDSFunc.DynamicData_get_string, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_ulong), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    (DDSFunc.DynamicData_get_wstring, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(ctypes.c_wchar_p), ctypes.POINTER(ctypes.c_ulong), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    (DDSFunc.DynamicData_set_string, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.c_char_p, DDS_DynamicDataMemberId, ctypes.c_char_p]),
    (DDSFunc.DynamicData_set_wstring, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.c_char_p, DDS_DynamicDataMemberId, ctypes.c_wchar_p]),    
    (DDSFunc.DynamicData_bind_complex_member, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDSType.DynamicData), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    (DDSFunc.DynamicData_unbind_complex_member, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDSType.DynamicData)]),
    (DDSFunc.DynamicData_get_member_type, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(ctypes.POINTER(DDSType.TypeCode)), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    (DDSFunc.DynamicData_get_member_count, None, DDS_UnsignedLong, [ctypes.POINTER(DDSType.DynamicData)]),
    (DDSFunc.DynamicData_get_type, check_null, ctypes.POINTER(DDSType.TypeCode), [ctypes.POINTER(DDSType.DynamicData)]),
    (DDSFunc.DynamicData_get_type_kind, None, enum, [ctypes.POINTER(DDSType.DynamicData)]),
    (DDSFunc.DynamicData_delete, None, None, [ctypes.POINTER(DDSType.DynamicData)]),
    
    (DDSFunc.DynamicDataWriter_narrow, check_null, ctypes.POINTER(DDSType.DynamicDataWriter), [ctypes.POINTER(DDSType.DataWriter)]),
    (DDSFunc.DynamicDataWriter_write, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataWriter), ctypes.POINTER(DDSType.DynamicData), ctypes.c_void_p]),
    
    (DDSFunc.DynamicDataReader_narrow, check_null, ctypes.POINTER(DDSType.DynamicDataReader), [ctypes.POINTER(DDSType.DataReader)]),
    (DDSFunc.DynamicDataReader_take, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataReader), ctypes.POINTER(DDSType.DynamicDataSeq), ctypes.POINTER(DDSType.SampleInfoSeq), DDS_Long, DDS_SampleStateMask, DDS_ViewStateMask, DDS_InstanceStateMask]),
    (DDSFunc.DynamicDataReader_return_loan, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicDataReader), ctypes.POINTER(DDSType.DynamicDataSeq), ctypes.POINTER(DDSType.SampleInfoSeq)]),
    
    (DDSFunc.TypeCode_name, check_ex, ctypes.c_char_p, [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDS_ExceptionCode_t)]),
    (DDSFunc.TypeCode_kind, check_ex, ctypes.c_ulong, [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDS_ExceptionCode_t)]),
    (DDSFunc.TypeCode_member_count, check_ex, ctypes.c_ulong, [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDS_ExceptionCode_t)]),
    (DDSFunc.TypeCode_member_name, check_ex, ctypes.c_char_p, [ctypes.POINTER(DDSType.TypeCode), ctypes.c_ulong, ctypes.POINTER(DDS_ExceptionCode_t)]),
    (DDSFunc.TypeCode_member_type, check_ex, ctypes.POINTER(DDSType.TypeCode), [ctypes.POINTER(DDSType.TypeCode), ctypes.c_ulong, ctypes.POINTER(DDS_ExceptionCode_t)]),
    
    (DDSFunc.DynamicDataSeq_initialize, None, ctypes.c_bool, [ctypes.POINTER(DDSType.DynamicDataSeq)]),
    (DDSFunc.DynamicDataSeq_get_length, None, ctypes.c_ulong, [ctypes.POINTER(DDSType.DynamicDataSeq)]),
    (DDSFunc.DynamicDataSeq_get_reference, check_null, ctypes.POINTER(DDSType.DynamicData), [ctypes.POINTER(DDSType.DynamicDataSeq), ctypes.c_long]),
    
    (DDSFunc.SampleInfoSeq_initialize, None, ctypes.c_bool, [ctypes.POINTER(DDSType.SampleInfoSeq)]),
])

del type(DDSFunc).__getattr__

class TCKind(object):
    NULL = 0
    SHORT = 1
    LONG = 2
    USHORT = 3
    ULONG = 4
    FLOAT = 5
    DOUBLE = 6
    BOOLEAN = 7 
    CHAR = 8
    OCTET = 9
    STRUCT = 10
    UNION = 11
    ENUM = 12
    STRING = 13
    SEQUENCE = 14
    ARRAY = 15
    ALIAS = 16
    LONGLONG = 17
    ULONGLONG = 18
    LONGDOUBLE = 19
    WCHAR = 20
    WSTRING = 21
    VALUE = 22
    SPARSE = 23
    RAW_BYTES = 0x7e
    RAW_BYTES_KEYED = 0x7f

def write_into_dd_member(obj, dd, member_name=None, member_id=DDS_DYNAMIC_DATA_MEMBER_ID_UNSPECIFIED):
    tc = ctypes.POINTER(DDSType.TypeCode)()
    dd.get_member_type(ctypes.byref(tc), member_name, member_id, ex())
    
    kind = tc.kind(ex())
    if kind == TCKind.SHORT:
        if not -2**15 <= obj < 2**15: raise ValueError()
        dd.set_short(member_name, member_id, obj)
    elif kind == TCKind.LONG:
        if not -2**31 <= obj < 2**31: raise ValueError()
        dd.set_long(member_name, member_id, obj)
    elif kind == TCKind.USHORT:
        if not 0 <= obj < 2**16: raise ValueError()
        dd.set_ushort(member_name, member_id, obj)
    elif kind == TCKind.ULONG:
        if not 0 <= obj < 2**32: raise ValueError()
        dd.set_ulong(member_name, member_id, obj)
    elif kind == TCKind.FLOAT:
        dd.set_float(member_name, member_id, obj)
    elif kind == TCKind.DOUBLE:
        dd.set_double(member_name, member_id, obj)
    elif kind == TCKind.BOOLEAN:
        dd.set_boolean(member_name, member_id, obj)
    elif kind == TCKind.CHAR:
        dd.set_char(member_name, member_id, obj)
    elif kind == TCKind.OCTET:
        if not 0 <= obj < 2**8: raise ValueError()
        dd.set_octet(member_name, member_id, obj)
    elif kind == TCKind.STRUCT or kind == TCKind.SEQUENCE or kind == TCKind.ARRAY:
        inner = DDSFunc.DynamicData_new(None, get('DYNAMIC_DATA_PROPERTY_DEFAULT', DDSType.DynamicDataProperty))
        try:
            dd.bind_complex_member(inner, member_name, member_id)
            try:
                write_into_dd(obj, inner)
            finally:
                dd.unbind_complex_member(inner)
        finally:
            inner.delete()
    elif kind == TCKind.STRING:
        if '\0' in obj:
            raise ValueError('strings can not contain null characters')
        dd.set_string(member_name, member_id, obj)
    elif kind == TCKind.LONGLONG:
        if not -2**63 <= obj < 2**63: raise ValueError()
        dd.set_ulonglong(member_name, member_id, obj)
    elif kind == TCKind.ULONGLONG:
        if not 0 <= obj < 2**64: raise ValueError()
        dd.set_ulonglong(member_name, member_id, obj)
    elif kind == TCKind.LONGDOUBLE:
        dd.set_longdouble(member_name, member_id, obj)
    elif kind == TCKind.WCHAR:
        dd.set_wchar(member_name, member_id, obj)
    elif kind == TCKind.WSTRING:
        dd.set_wstring(member_name, member_id, obj)
    else:
        raise NotImplementedError(kind)

def write_into_dd(obj, dd):
    kind = dd.get_type_kind()
    if kind == TCKind.STRUCT:
        assert isinstance(obj, dict)
        tc = dd.get_type()
        for i in xrange(tc.member_count(ex())):
            name = tc.member_name(i, ex())
            write_into_dd_member(obj[name], dd, member_name=name)
    elif kind == TCKind.ARRAY or kind == TCKind.SEQUENCE:
        assert isinstance(obj, list)
        for i, x in enumerate(obj):
            write_into_dd_member(x, dd, member_id=i+1)
    else:
        raise NotImplementedError(kind)

def unpack_dd_member(dd, member_name=None, member_id=DDS_DYNAMIC_DATA_MEMBER_ID_UNSPECIFIED):
    tc = ctypes.POINTER(DDSType.TypeCode)()
    dd.get_member_type(ctypes.byref(tc), member_name, member_id, ex())
    
    kind = tc.kind(ex())
    if kind == TCKind.SHORT:
        inner = DDS_Short()
        dd.get_short(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.LONG:
        inner = DDS_Long()
        dd.get_long(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.USHORT:
        inner = DDS_UnsignedShort()
        dd.get_ushort(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.ULONG:
        inner = DDS_UnsignedLong()
        dd.get_ulong(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.FLOAT:
        inner = DDS_Float()
        dd.get_float(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.DOUBLE:
        inner = DDS_Double()
        dd.get_double(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.BOOLEAN:
        inner = DDS_Boolean()
        dd.get_boolean(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.CHAR:
        inner = DDS_Char()
        dd.get_char(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.OCTET:
        inner = DDS_Octet()
        dd.get_octet(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.STRUCT or kind == TCKind.SEQUENCE or kind == TCKind.ARRAY:
        inner = DDSFunc.DynamicData_new(None, get('DYNAMIC_DATA_PROPERTY_DEFAULT', DDSType.DynamicDataProperty))
        try:
            dd.bind_complex_member(inner, member_name, member_id)
            try:
                return unpack_dd(inner)
            finally:
                dd.unbind_complex_member(inner)
        finally:
            inner.delete()
    elif kind == TCKind.STRING:
        inner = ctypes.c_char_p(None)
        inner_size = ctypes.c_ulong(0)
        dd.get_string(ctypes.byref(inner), ctypes.byref(inner_size), member_name, member_id)
        return inner.value[:inner_size.value]
    elif kind == TCKind.LONGLONG:
        inner = DDS_LongLong()
        dd.get_longlong(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.ULONGLONG:
        inner = DDS_UnsignedLongLong()
        dd.get_ulonglong(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.LONGDOUBLE:
        inner = DDS_LongDouble()
        dd.get_longdouble(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.WCHAR:
        inner = DDS_Wchar()
        dd.get_wchar(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.WSTRING:
        inner = ctypes.c_wchar_p(None)
        inner_size = ctypes.c_ulong(0) # XXX not sure what this refers to - number of bytes or characters?
        dd.get_wstring(ctypes.byref(inner), ctypes.byref(inner_size), member_name, member_id)
        return inner.value[:inner_size.value]
    else:
        raise NotImplementedError(kind)

def unpack_dd(dd):
    kind = dd.get_type_kind()
    if kind == TCKind.STRUCT:
        obj = {}
        tc = dd.get_type()
        for i in xrange(tc.member_count(ex())):
            name = tc.member_name(i, ex())
            obj[name] = unpack_dd_member(dd, member_name=name)
        return obj
    elif kind == TCKind.ARRAY or kind == TCKind.SEQUENCE:
        obj = []
        for i in xrange(dd.get_member_count()):
            obj.append(unpack_dd_member(dd, member_id=i+1))
        return obj
    else:
        raise NotImplementedError(kind)

class Topic(object):
    def __init__(self, dds, name, data_type):
        self._dds = dds
        self.name = name
        self._data_type = data_type
        del dds, name, data_type
        
        self._support = DDSFunc.DynamicDataTypeSupport_new(self._data_type._get_typecode(), DDSVoidP.DYNAMIC_DATA_TYPE_PROPERTY_DEFAULT)
        self._support.register_type(self._dds._participant, self._data_type.name)
        
        self._topic = self._dds._participant.create_topic(
            self.name,
            self._data_type.name,
            DDSVoidP.TOPIC_QOS_DEFAULT,
            None,
            0,
        )
        
        self._writer = self._dds._publisher.create_datawriter(
            self._topic,
            DDSVoidP.DATAWRITER_QOS_DEFAULT,
            None,
            0,
        )
        self._dyn_narrowed_writer = DDSFunc.DynamicDataWriter_narrow(self._writer)
        
        self._reader = self._dds._subscriber.create_datareader(
            self._topic.as_topicdescription(),
            DDSVoidP.DATAREADER_QOS_DEFAULT,
            None,
            0,
        )
        self._dyn_narrowed_reader = DDSFunc.DynamicDataReader_narrow(self._reader)
    
    def send(self, msg):
        sample = self._support.create_data()
        
        try:
            write_into_dd(msg, sample)
            self._dyn_narrowed_writer.write(sample, ctypes.create_string_buffer(struct.pack('<16sII', '', 16, 0))) # XXX ugly
        finally:
            self._support.delete_data(sample)
    
    def recv(self):
        data_seq = DDSType.DynamicDataSeq()
        DDSFunc.DynamicDataSeq_initialize(data_seq)
        info_seq = DDSType.SampleInfoSeq()
        DDSFunc.SampleInfoSeq_initialize(info_seq)
        self._dyn_narrowed_reader.take(ctypes.byref(data_seq), ctypes.byref(info_seq), 1, get('ANY_SAMPLE_STATE', DDS_SampleStateMask), get('ANY_VIEW_STATE', DDS_ViewStateMask), get('ANY_INSTANCE_STATE', DDS_InstanceStateMask))
        try:
            return unpack_dd(data_seq.get_reference(0))
        finally:
            self._dyn_narrowed_reader.return_loan(ctypes.byref(data_seq), ctypes.byref(info_seq))
    
    def __del__(self):
        self._dds._publisher.delete_datawriter(self._writer)
        self._dds._subscriber.delete_datareader(self._reader)
        self._dds._participant.delete_topic(self._topic)
        self._support.unregister_type(self._dds._participant, self._data_type.name) # XXX might not be safe if register_type was called multiple times
        self._support.delete()

class DDS(object):
    def __init__(self, domain_id=0):
        self._participant = DDSFunc.DomainParticipantFactory_get_instance().create_participant(
            domain_id,
            DDSVoidP.PARTICIPANT_QOS_DEFAULT,
            None,
            0,
        )
        
        self._publisher = self._participant.create_publisher(
            DDSVoidP.PUBLISHER_QOS_DEFAULT,
            None,
            0,
        )
        
        self._subscriber = self._participant.create_subscriber(
            DDSVoidP.SUBSCRIBER_QOS_DEFAULT,
            None,
            0,
        )
    
    def get_topic(self, name, data_type):
        # XXX cache this to handle it being called multiple times
        return Topic(self, name, data_type)
    
    def __del__(self):
        self._participant.delete_subscriber(self._subscriber)
        self._participant.delete_publisher(self._publisher)
        
        # very slow for some reason
        DDSFunc.DomainParticipantFactory_get_instance().delete_participant(self._participant)


class LibraryType(object):
    def __init__(self, lib, name):
        self._lib, self.name = lib, name
        del lib, name
        
        assert self._get_typecode().name(ex()) == self.name
    
    def _get_typecode(self):
        f = getattr(self._lib, self.name + '_get_typecode')
        f.argtypes = []
        f.restype = ctypes.POINTER(DDSType.TypeCode)
        f.errcheck = check_null
        return f()

class Library(object):
    def __init__(self, so_path):
        self._lib = ctypes.CDLL(so_path)
    
    def __getattr__(self, attr):
        return LibraryType(self._lib, attr)
