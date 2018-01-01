// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/coding/set_encoder.h"

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <zdict.h>

#include <fcntl.h>
#include <sys/stat.h>

#include "base/endian.h"
#include "base/hash.h"
#include "base/logging.h"

#include "strings/join.h"
#include "base/flit.h"

#ifndef IS_LITTLE_ENDIAN
#error this file assumes little endian architecture
#endif

using namespace std;
using strings::ByteRange;
using namespace base;

namespace util {

namespace {


constexpr uint16_t kMagic = 0x2ca7;
constexpr uint32_t kMaxHeaderSize = 11;  // including kMagic + flags
constexpr uint32_t SEQ_BLOCK_LOG_SIZE = 17;
constexpr uint32_t SEQ_BLOCK_SIZE = 1U << SEQ_BLOCK_LOG_SIZE;

// We will store 8 times more data for analysis than we gonna store in compressed blocks.
constexpr uint32_t MAX_BATCH_SIZE = SEQ_BLOCK_SIZE * 8;

constexpr uint32_t SEQ_DICT_MAX_SIZE = 1U << 17;


constexpr unsigned kArrLengthLimit = 1 << 14;  // to accommodate at least 1 array in 128KB.
constexpr unsigned kLenLimit = (1 << 16);

inline uint8* Ensure(size_t sz, base::PODArray<uint8>* dest) {
  size_t p = dest->size();
  dest->resize(p + sz);
  return dest->begin() + p;
}


#define CHECK_ZSTDERR(res) do { auto foo = (res); \
    CHECK(!ZSTD_isError(foo)) << ZSTD_getErrorName(foo); } while(false)

inline uint32_t InlineCode(uint32_t len) { return (len << 1) | 0;}
inline uint32_t DictCode(uint32_t id) { return (id << 1) | 1;}
inline bool IsDictCode(uint32_t code) { return (code & 1) != 0;}

constexpr uint32_t kDictHeaderSize = 6;

#if 0
static uint8_t *svb_encode_scalar(const uint16_t *in,
    uint8_t *__restrict__ key_ptr, uint8_t *__restrict__ data_ptr,
    uint32_t count) {
  if (count == 0)
    return data_ptr; // exit immediately if no data

  uint8_t shift = 0; // cycles 0, 2, 4, 6, 0, 2, 4, 6, ...
  uint8_t key = 0;
  for (uint32_t c = 0; c < count; c++) {
    if (shift == 8) {
      shift = 0;
      *key_ptr++ = key;
      key = 0;
    }
    uint32_t val = in[c];
    uint8_t code = _encode_data(val, &data_ptr);
    key |= code << shift;
    shift += 1;
  }

  *key_ptr = key;  // write last key (no increment needed)
  return data_ptr; // pointer to first unused data byte
}

// Encode an array of a given length read from in to bout in streamvbyte format.
// Returns the number of bytes written.
// Taken from: https://github.com/lemire/streamvbyte/blob/master/src/streamvbyte.c
size_t streamvbyte_encode(const uint16_t *in, uint32_t count, uint8_t *out) {
  uint8_t *keyPtr = out;
  uint32_t keyLen = (count + 7) / 8; // 1-bits rounded to full byte
  uint8_t *dataPtr = keyPtr + keyLen; // variable byte data after all keys
  return svb_encode_scalar(in, keyPtr, dataPtr, count) - out;
}


uint8* encode_pair(uint16 a, uint16 b, uint8* dest) {
  uint32 v = (a & 0xf) | ((b & 0xf) << 4);
  a >>= 4;
  b >>= 4;

  if (a || b) {
    v |= ((a & 0xf) | ((b & 0xf) << 4)) << 8;
  }

  return Varint::Encode32(dest, v);
}
#endif


}  // namespace


// We both delta encoding ordered set of unique numbers and shrink it with partial RLE.
// The RLE uses the fact that the input numbers are smaller than 16K and it uses sometimes
// LSB bit to encode repeat value. The rule is that if the destination symbol is less than
// kSmallNum then the next destination symbol shifted left by 1 and reserves LSB to say
// whether it's repeat or not. See header for more info.
unsigned DeltaEncode16(const uint16* src, unsigned cnt, uint16* dest) {
  if (VLOG_IS_ON(3)) {
    string tmp = JoinElementsIterator(src, src + cnt, ",");
    LOG(INFO) << "Adding " << cnt << ": " << tmp;
  }
  uint16* dest_orig = dest;

  *dest++ = *src;
  if (cnt == 1)
    return 1;

  const uint16* end = src + cnt;
  uint16 prev = *src++;
  uint16 prev_dest = prev;

  unsigned rep = 0;
  for (; src < end; ++src) {
    DCHECK_LT(prev, *src);

    unsigned is_prev_dest_small = (prev_dest < internal::kSmallNum);
    uint16 delta = *src - prev - 1;
    prev = *src;
    if (delta == prev_dest && is_prev_dest_small) {
      ++rep;
      continue;
    }

    if (rep) {
      if (rep > 1) {
        *dest++ = ((rep - 1) << 1) | 1;
        *dest++ = delta;
      } else {
        *dest++ = prev_dest << 1;
        *dest++ = delta << 1;
      }
      rep = 0;
    } else {
      // Store delta.
      *dest++ = (delta << is_prev_dest_small);
    }
    prev_dest = delta;
  }

  if (rep) {
    if (rep > 1)
      *dest++ = ((rep - 1) << 1) | 1;
    else
      *dest++ = prev_dest << 1;
  }

  return dest - dest_orig;
}

// Returns (num symbols, num of bytes) pair
uint32_t DeltaAndFlitEncode(LiteralDictBase::SymbId* src, uint32_t cnt,
                            LiteralDictBase::SymbId* tmp,
                            void* dest) {
  using SymbId = LiteralDictBase::SymbId;
  std::sort(src, src + cnt);

  uint32_t out_sz = DeltaEncode16(src, cnt, tmp);

  DCHECK_LE(out_sz, cnt);  // it always holds due to how DeltaEncode16 works.

  uint8* start = static_cast<uint8_t*>(dest);
  uint8* next = start;
  SymbId current = tmp[0];
  for (unsigned j = 1; j < out_sz; ++j) {
    // EncodeT uncoditionally writes at least 4 bytes so we store the next value.
    SymbId val = tmp[j];
    next += flit::EncodeT<uint32_t>(current, next);
    current = val;
  }
  next += flit::EncodeT<uint32_t>(current, next);

  return next - start;
}


template<typename T> void LiteralDict<T>::Build() {
  using iterator = typename decltype(freq_map_)::iterator;

  vector<pair<unsigned, iterator>> freq_arr;
  freq_arr.reserve(freq_map_.size());

  for (auto it = begin(freq_map_); it != end(freq_map_); ++it) {
    freq_arr.emplace_back(it->second.cnt, it);
  }
  std::sort(begin(freq_arr), end(freq_arr),
    [](const auto& val1, const auto& val2) { return val1.first > val2.first; }
  );

  alphabet_.resize(freq_arr.size());

  for (unsigned i = 0; i < freq_arr.size(); ++i) {
    const iterator& it = freq_arr[i].second;
    T t = it->first;
    alphabet_[i] = t;
    it->second.id = i;
  }
}

template<typename T> size_t LiteralDict<T>::GetMaxSerializedSize() const {
  return sizeof(T) * alphabet_.size();
}

template<typename T> size_t LiteralDict<T>::SerializeTo(uint8_t* dest) const {
  uint8_t* ptr = dest;

  for (const auto& t : alphabet_) {
    LittleEndian::StoreT(typename std::make_unsigned<T>::type(t), ptr);
    ptr += sizeof(T);
  }
  return ptr - dest;
}

template<typename T> bool LiteralDict<T>::Resolve(const T* src, uint32_t count, SymbId* dest) {
  for (uint32_t i = 0; i < count; ++i) {
    auto res = freq_map_.emplace(src[i], Record{});
    if (res.second) {
      if (alphabet_.size() >= kMaxAlphabetSize) {
        freq_map_.erase(res.first);
        return false;
      }

      res.first->second.id = alphabet_.size();
      alphabet_.push_back(src[i]);
    }

    LittleEndian::Store16(dest + i, res.first->second.id);
  }
  return true;
}


ostream& operator<<(ostream& os, const ZSTD_parameters& p) {
  os << "wlog: " << p.cParams.windowLog << ", clog: " << p.cParams.chainLog << ", strategy: "
     << p.cParams.strategy << ", slen: " << p.cParams.searchLength << ", cntflag: "
     << p.fParams.contentSizeFlag << ", hashlog: " << p.cParams.hashLog;
  return os;
}

struct SeqEncoderBase::ZstdCntx {
  ZSTD_CCtx* context;


  ZstdCntx(const ZstdCntx&) = delete;
  void operator=(const ZstdCntx&) = delete;

  ZstdCntx() {
    context = ZSTD_createCCtx();
  }

  ~ZstdCntx() {
    ZSTD_freeCCtx(context);
  }

  bool start = true;
};


void BlockHeader::Read(const uint8_t* src) {
  flags = *src;

  const uint8_t* next = src + 1;
  if (flags & kDictBit) {
    num_sequences = LittleEndian::Load16(next);
    byte_len_size_comprs = LittleEndian::Load24(next + 2);
    next += 5;
  } else {
    num_sequences = byte_len_size_comprs = 0;
  }
  sequence_size_comprs = LittleEndian::Load24(next);
}

uint8 BlockHeader::Write(uint8_t* dest) const {
  LittleEndian::Store16(dest, kMagic);

  dest[2] = flags;  // type
  uint8* next = dest + 3;
  if (flags & kDictBit) {
    LittleEndian::Store16(next, num_sequences);
    LittleEndian::Store24(next + 2, byte_len_size_comprs);
    next += 5;
  }
  LittleEndian::Store24(next, sequence_size_comprs);
  return next - dest + 3;
}

// not including magic but including flags.
uint8_t BlockHeader::HeaderSize(uint8_t flags) {
  if (flags & kDictBit) {
    return 9;
  }
  return 4;
}

struct SeqDecoderBase::Zstd {
  ZSTD_DCtx* context;
  uint8_t start = 2;  // bit 1 - start, bit 0 - which destination buffer to fill.

  Zstd(const Zstd&) = delete;
  void operator=(const Zstd&) = delete;

  Zstd() {
    context = ZSTD_createDCtx();
  }

  ~Zstd() {
    ZSTD_freeDCtx(context);
  }

  size_t offset() const { return SEQ_BLOCK_SIZE * (start & 1); }
};

SeqEncoderBase::SeqEncoderBase() {
  seq_map_.set_empty_key(StringPiece());
  dict_seq_map_.set_empty_key(strings::ByteRange());

  prev_block_.reserve(SEQ_BLOCK_SIZE);

  zstd_cntx_.reset(new ZstdCntx);
  tmp_symb_.reserve(kArrLengthLimit);
}

SeqEncoderBase::~SeqEncoderBase() {
}

void SeqEncoderBase::AddCompressedBuf(const BlockHeader& bh) {
  VLOG(1) << "Adding compressed block: " << bh.num_sequences
          << "/" << bh.byte_len_size_comprs << "/" << bh.sequence_size_comprs
          << "/" << int(bh.flags);

  size_t blob_sz = bh.byte_len_size_comprs + bh.sequence_size_comprs;
  std::unique_ptr<uint8_t[]> cb(new uint8_t[blob_sz + kMaxHeaderSize]);

  uint8_t offset = bh.Write(cb.get());
  memcpy(cb.get() + offset, compress_data_.begin(), blob_sz);

  compressed_blocks_.emplace_back(cb.get(), blob_sz + offset);
  compressed_bufs_.emplace_back(std::move(cb));
}

bool SeqEncoderBase::LearnSeqDict(strings::ByteRange key) {
  auto res = seq_map_.emplace(key, EntryVal{});
  EntryVal& ev = res.first->second;
  ev.ref_cnt++;

  if (!res.second) {
    const uint8_t* ptr = res.first->first.data();
    DCHECK_GE(ptr, lit_data_.data());
    DCHECK_LT(ptr, lit_data_.data() + lit_data_.capacity());

    duplicate_seq_.push_back(ptr - lit_data_.data());
  }
  return res.second;
}


uint32 SeqEncoderBase::Cost() const {
  uint32_t cost = lit_data_.size();

  for (auto block : compressed_blocks_) {
    cost += block.size();
  }

  if (state_ == LIT_DICT) {
    cost += len_code_.size() * sizeof(uint16_t);
  }
  return cost;
}

void SeqEncoderBase::AnalyzePreDict() {
  uint32_t alplhabet_size = PrepareDict();
  if (state_ == NO_LIT_DICT)
    return;

  uint32_t lit_cnt = lit_data_.size() / sizeof(SymbId);

  SymbId* symb_arr = reinterpret_cast<SymbId*>(lit_data_.data());

  uint8_t* dest = lit_data_.begin();
  uint32_t delta_flit_bytes = 0;

  for (size_t i = 0; i < len_code_.size(); ++i) {
    uint32_t lit_num = len_code_[i];

    uint32_t bytes_num = DeltaAndFlitEncode(symb_arr, lit_num, tmp_symb_.begin(), dest);

    // it always holds due to how DeltaEncode16 and flit encoding work with 14 bit numbers.
    DCHECK_LE(bytes_num, lit_num * sizeof(SymbId));

    bool new_entry = disable_seq_dict_ || LearnSeqDict(ByteRange(dest, bytes_num));

    if (new_entry) {
      dest += bytes_num;
      len_code_[i] = InlineCode(bytes_num);
    } else {
      len_code_[i] = DictCode(bytes_num);
    }

    symb_arr += lit_num;
    delta_flit_bytes += bytes_num;
  }

  size_t delta_flit_cost = alplhabet_size * literal_size_ + delta_flit_bytes;

  // TODO: to refine the state machine: to allow variations of
  // delta-flit / sequence dictionary encodings.
  CHECK_LT(delta_flit_cost, literal_size_ * lit_cnt) << "TBD to fallback.";

  size_t seq_size = dest - lit_data_.data();
  lit_data_.resize_assume_reserved(seq_size);

  // TODO: Is that the right place to do it? Maybe it's worth to wait until lit_data is full
  // and then to analyze it? PRO this checl - faster fallback to faster heuristic on uncompressable
  // data.
  if (seq_size + seq_map_.size() * 2 > delta_flit_bytes) {
    VLOG(1) << "Falling back to no sequence. Reason: " << seq_size + seq_map_.size() * 2
            << " vs " << delta_flit_bytes;
    for (auto& k_v : seq_map_) {
      k_v.second.ref_cnt = 0;
    }

    // will flush and clear using sequence dictionaries.
    AnalyzeSequenceDict();
  }

  VLOG(1) << "original/flit: " << added_lit_cnt_ * literal_size_ << "/" << lit_data_.size();
}

void SeqEncoderBase::CompressRawLit(bool final) {
  DCHECK_EQ(state_, NO_LIT_DICT);

  size_t csz1 = ZSTD_compressBound(lit_data_.size());

  compress_data_.reserve(csz1);
  size_t res = ZSTD_compress(compress_data_.begin(), csz1,
                             lit_data_.data(), lit_data_.size(), 1);
  CHECK(!ZSTD_isError(res)) << ZSTD_getErrorName(res);
  VLOG(1) << "CompressRawLit: from " << lit_data_.size() << " to " << res;

  BlockHeader bh;
  bh.flags = (final ? BlockHeader::kFinalBit : 0);

  bh.sequence_size_comprs = res;

  AddCompressedBuf(bh);

  lit_data_.clear();
  len_code_.clear();
}

void SeqEncoderBase::CompressFlitSequences(bool final) {
  DCHECK_EQ(state_, LIT_DICT);
  CHECK_LT(len_code_.size(), kLenLimit);

  // We support arrays upto kArrLengthLimit elements so 3 bytes per len value is enough.
  tmp_space_.reserve(len_code_.size() * 3);

  size_t len_size = 0;
  for (const uint32_t val : len_code_) {
    len_size += flit::EncodeT<uint32_t>(val, tmp_space_.data() + len_size);
  }
  CHECK_LE(len_size, tmp_space_.capacity());

  BlockHeader bh;
  bh.flags = BlockHeader::kDictBit | (final ? BlockHeader::kFinalBit : 0);
  bh.num_sequences = len_code_.size();

  if (!dict_seq_.empty()) {
    bh.flags |= BlockHeader::kDictSeqBit;
  }

  size_t csz1 = ZSTD_compressBound(len_size);
  size_t csz2 = ZSTD_compressBound(lit_data_.size());

  size_t upper_bound = csz1 + csz2;
  compress_data_.reserve(upper_bound);

  uint8_t* compress_pos = compress_data_.begin();

  size_t res = ZSTD_compress(compress_pos, csz1,
                             tmp_space_.data(), len_size, 1);
  CHECK(!ZSTD_isError(res)) << ZSTD_getErrorName(res);
  bh.byte_len_size_comprs = res;

  compress_pos += res;
  if (zstd_cntx_->start) {
    CHECK(compressed_blocks_.empty() && compressed_bufs_.empty());

#if 0
    if (!dict_seq_.empty()) {
      zstd_dict_.reset(new uint8[1 << 17]);
      std::vector<size_t> ss(dict_seq_.len_array().begin(), dict_seq_.len_array().end());

      zstd_dict_size_ = ZDICT_trainFromBuffer(zstd_dict_.get(), 1 << 17,
                                              dict_seq_.data().data(), ss.data(), ss.size());
      VLOG(1) << "Zdict dictsize: " << zstd_dict_size_;
    } else {
      zstd_dict_size_ = 0;
    }
#endif
    ZSTD_parameters params{ZSTD_getCParams(6, 0 /* est src size*/, zstd_dict_size_),
                           ZSTD_frameParameters()};

    params.cParams.windowLog = SEQ_BLOCK_LOG_SIZE + 1;
    params.cParams.hashLog = SEQ_BLOCK_LOG_SIZE - 2;
    // params.cParams.chainLog = SEQ_BLOCK_LOG_SIZE;

    VLOG(1) << "Using: " << params;

    CHECK_ZSTDERR(ZSTD_compressBegin_advanced(
        zstd_cntx_->context, zstd_dict_.get(), zstd_dict_size_, params, ZSTD_CONTENTSIZE_UNKNOWN));
    zstd_cntx_->start = false;
  }

  bool finish_frame = final;

  auto func = finish_frame ? ZSTD_compressEnd : ZSTD_compressContinue;
  res = func(zstd_cntx_->context, compress_pos, csz2, lit_data_.data(), lit_data_.size());
  CHECK_ZSTDERR(res);
  bh.sequence_size_comprs = res;
  compress_pos += res;

  size_t real_size = compress_pos - compress_data_.begin();
  VLOG(1) << "flit/compressed: " << lit_data_.size() << "/" << real_size + kMaxHeaderSize;

  CHECK_LE(real_size, upper_bound);

  AddCompressedBuf(bh);

  if (finish_frame) {
    VLOG(1) << "SeqDict referenced " << dict_ref_bytes_;
    zstd_cntx_->start = true;
    dict_ref_bytes_ = 0;
  }

  lit_data_.swap(prev_block_);
  lit_data_.clear();
  len_code_.clear();
  added_lit_cnt_ = 0;
}

void SeqEncoderBase::AnalyzeSequenceDict() {
  using iterator = decltype(seq_map_)::iterator;
  std::vector<pair<unsigned, iterator>> sorted_it;

  // Add key to key_order_ to maintain the original order of the keys.
  for (auto it = seq_map_.begin(); it != seq_map_.end(); ++it) {
    if (it->first.size() > 1 && it->second.ref_cnt > 1)
      sorted_it.emplace_back(it->second.ref_cnt, it);
    else
      it->second.ref_cnt = 0; // just reset it.
  }

  std::sort(sorted_it.begin(), sorted_it.end(),
    [](const pair<unsigned, iterator>& p1, const pair<unsigned, iterator>& p2) {
        return p1.first > p2.first;
    });

  size_t selected_size = 0, ref_bytes = 0;

  auto it = sorted_it.begin();

  // Copy into dict_seq_ entries with larger reference count first.
  for (; it != sorted_it.end(); ++it) {
    ByteRange seq_str = it->second->first;
    EntryVal& ev = it->second->second;

    if (selected_size + seq_str.size() >= SEQ_DICT_MAX_SIZE) {
      break;
    }
    VLOG(2) << "Choosen dict with factor " << ev.ref_cnt << ", size " << seq_str.size();

    // Returns the sequence id of the added sequence seq_str.
    ev.dict_id = dict_seq_.Add(seq_str.begin(), seq_str.end());

    selected_size += seq_str.size();

    // How many bytes reference this entry.
    ref_bytes += seq_str.size() * ev.ref_cnt;
  }
  dict_nominal_ratio_ = ref_bytes * 1.0 / (selected_size + 1);

  size_t dict_count = it - sorted_it.begin();

  VLOG(1) << "Dictionary will take " << selected_size << " bytes and will represent "
          << ref_bytes << " bytes with " << dict_count << " items, leaving "
          << sorted_it.size() - dict_count;
  VLOG(1) << "original/flit: " << added_lit_cnt_ * literal_size_ << "/" << lit_data_.size();

  // We gonna proceed with sequence dictionary.
  for (; it != sorted_it.end(); ++it) {
    it->second->second.ref_cnt = 0;
  }

  {
    // Create reverse mapping to dict_seq.
    unsigned dict_index = 0;
    CHECK(dict_seq_map_.empty());
    for (auto it = dict_seq_.begin(); it != dict_seq_.end(); ++it) {
      auto res = dict_seq_map_.emplace(*it, dict_index++);
      CHECK(res.second);
    }
  }

  uint32_t inline_index = 0, dest_index = 0, duplicate_index = 0;
  base::PODArray<uint8> seq_data(pmr::get_default_resource());
  seq_data.reserve(SEQ_BLOCK_SIZE);
  seq_data.swap(lit_data_);

  const uint8* key_src = seq_data.data();
  size_t len_size = len_code_.size();

  ByteRange ref_entry;

  // now lit data has only SEQ_BLOCK_SIZE capacity and seq_data has all the data.
  // I reuse len_code_ for both reading and writing using the fact that pod array
  // does not change memory when resizing the array.
  dict_ref_bytes_ = 0;
  for (size_t i = 0; i < len_size; ++i, ++dest_index) {
    uint32_t len_code = len_code_[i];
    uint32_t len = len_code >> 1;

    if (IsDictCode(len_code)) {
      ref_entry.reset(seq_data.data() + duplicate_seq_[duplicate_index++], len);
    } else {
      ref_entry.reset(key_src, len);
      key_src += len;
    }

    auto it = seq_map_.find(ref_entry);

    CHECK(it != seq_map_.end());
    const EntryVal& ev = it->second;

    // Copy into dict_seq_ entries with larger reference count first.
    if (ev.ref_cnt < 2) {
      if (len + lit_data_.size() > SEQ_BLOCK_SIZE ||
        dest_index >= kLenLimit) {
        len_code_.resize_assume_reserved(dest_index);
        CompressFlitSequences(false);
        dest_index = 0;
      }

      lit_data_.insert(ref_entry.begin(), ref_entry.end());
      ++inline_index;

      len_code_[dest_index] = InlineCode(len);
    } else {
      dict_ref_bytes_ += len;
      len_code_[dest_index] = DictCode(ev.dict_id);
    }
  }
  CHECK_EQ(duplicate_index, duplicate_seq_.size());
  CHECK_EQ(dict_ref_bytes_, ref_bytes);

  seq_map_.clear();
  len_code_.resize_assume_reserved(dest_index);
}

bool SeqEncoderBase::PrepareForSymbAvailability(uint32_t cnt) {
  DCHECK_LE(cnt * sizeof(SymbId), lit_data_.capacity());

  // lit_data_.capacity() changes depending on the state of the encoder.
  // It will be MAX_BATCH_SIZE when we analyze literal and sequence dictionaries.
  // It will SEQ_BLOCK_SIZE once the dictionaries are determined.
  DCHECK(lit_data_.capacity() == SEQ_BLOCK_SIZE || lit_data_.capacity() == MAX_BATCH_SIZE);
  DCHECK(!seq_map_.empty() || lit_data_.capacity() == SEQ_BLOCK_SIZE);

  if (cnt * sizeof(SymbId) + lit_data_.size() > lit_data_.capacity()) {
    if (!seq_map_.empty()) {
      // Decide whether to use sequence dictionary.
      AnalyzeSequenceDict();
      return false;
    }
    CHECK_EQ(LIT_DICT, state_);
    CompressFlitSequences(false);
  }
  return true;
}

void SeqEncoderBase::BacktrackToRaw() {
  if (lit_data_.size() >= SEQ_BLOCK_SIZE / 8 ) {
    // There is enough intermediate data to create last dictionary based block.
    CompressFlitSequences(false);
  } else {
    LOG(FATAL) << "TBD_INFLATE the current buffer back to raw literals";
  }
  state_ = NO_LIT_DICT;
}

void SeqEncoderBase::AddEncodedSymbols(SymbId* src, uint32 cnt) {
  uint8* next = end(lit_data_);

  uint32_t bytes_num = DeltaAndFlitEncode(src, cnt, tmp_symb_.begin(), next);

  ByteRange candidate(next, bytes_num);
  uint32_t code = InlineCode(bytes_num);

  if (!seq_map_.empty()) {
    bool new_entry = LearnSeqDict(candidate);

    if (!new_entry) {
      code = DictCode(bytes_num);
      bytes_num = 0;
    }
  } else if (!dict_seq_map_.empty()) {
    auto it = dict_seq_map_.find(candidate);
    if (it != dict_seq_map_.end()) {
      code = DictCode(it->second);
      dict_ref_bytes_ += bytes_num;
      bytes_num = 0;
    }
  } else {
    // just regular LIT_DICT, no seq learning or resolving.
  }
  len_code_.push_back(code);

  lit_data_.resize_assume_reserved(lit_data_.size() + bytes_num);
}

void SeqEncoderBase::Flush() {
  if (state_ == PRE_DICT) {
    AnalyzePreDict();
  }

  if (!seq_map_.empty()) {
    AnalyzeSequenceDict();
  }

  if (state_ == LIT_DICT) {
    CompressFlitSequences(true);
  } else {
    CompressRawLit(true);
  }
}


/****************************************************************
 SeqEncoder
******************************************************************************/
template <size_t INT_SIZE> SeqEncoder<INT_SIZE>::SeqEncoder() {
  literal_size_ = INT_SIZE;
}


// TODO: fix reinterpret_cast issues (strict aliasing rules are broken).
template <size_t INT_SIZE> void SeqEncoder<INT_SIZE>::Add(const UT* src, unsigned cnt) {
  CHECK_LT(cnt, kArrLengthLimit);
  CHECK_GT(cnt, 0);

  size_t added_bytes = cnt * INT_SIZE;
  bool finished = true;
  do {
    switch (state_) {
      case PRE_DICT:
        if (added_bytes + lit_data_.size() <= lit_data_.capacity()) {
          uint8* dest = lit_data_.end();
          memcpy(dest, src, INT_SIZE * cnt);  // Assuming little endian.
          len_code_.push_back(cnt);
          lit_data_.resize(lit_data_.size() + added_bytes);
          finished = true;
        } else {
          if (lit_data_.capacity() == 0) {
            lit_data_.reserve(disable_seq_dict_ ? SEQ_BLOCK_SIZE : MAX_BATCH_SIZE);
          } else {
            // We have full batch ready.
            AnalyzePreDict();
          }
          finished = false;
        }
      break;
      case LIT_DICT:
        finished = AddDictEncoded(src, cnt);
      break;
      case NO_LIT_DICT:
        LOG(FATAL) << "TBD";
      break;
    }
  } while (!finished);

  added_lit_cnt_ += cnt;
}

template <size_t INT_SIZE> bool SeqEncoder<INT_SIZE>::GetDictSerialized(std::string* dest) {
  if (state_ == NO_LIT_DICT)
    return false;

  size_t max_size = kDictHeaderSize + dict_.GetMaxSerializedSize();
  if (!dict_seq_.empty()) {
    max_size += dict_seq_.GetMaxSerializedSize();
  }

  dest->resize(max_size);

  uint8_t* start = reinterpret_cast<uint8_t*>(&dest->front());
  size_t dict_sz = dict_.SerializeTo(start + kDictHeaderSize);

  size_t seq_sz = 0;
  LittleEndian::Store24(start, dict_sz);
  if (!dict_seq_.empty()) {
    seq_sz = dict_seq_.SerializeTo(start + kDictHeaderSize + dict_sz);
  }
  LittleEndian::Store24(start + 3, seq_sz);

  dest->resize(dict_sz + seq_sz + kDictHeaderSize);

  return true;
}

template <size_t INT_SIZE> bool SeqEncoder<INT_SIZE>::AddDictEncoded(const UT* src, unsigned cnt) {
  if (!PrepareForSymbAvailability(cnt))
    return false;

  // TODO: to resolve strict aliasing issues.
  // SymbId* begin = reinterpret_cast<SymbId*>(end(lit_data_));

  if (!dict_.Resolve(src, cnt, tmp_symb_.begin())) {
    // Dictionary is too large - we should to fallback.
    BacktrackToRaw();
    return false;
  }
  AddEncodedSymbols(tmp_symb_.begin(), cnt);

  return true;
}

template <size_t INT_SIZE> uint32_t SeqEncoder<INT_SIZE>::PrepareDict() {
  CHECK_EQ(0, dict_.alphabet_size());

  constexpr size_t kLiteralSize = INT_SIZE;

  uint32_t lit_cnt = lit_data_.size() / kLiteralSize;

  const UT* src = reinterpret_cast<UT*>(lit_data_.data());
  for (uint32_t i = 0; i < lit_cnt; ++i) {
    dict_.Add(src[i]);
  }

  if (dict_.dict_size() >= LiteralDictBase::kMaxAlphabetSize || dict_.dict_size() >= lit_cnt/2) {
    state_ = NO_LIT_DICT;
    dict_.Clear();
    return 0;
  }

  dict_.Build();
  state_ = LIT_DICT;

  SymbId* dest = reinterpret_cast<SymbId*>(lit_data_.data());

  for (uint32_t i = 0; i < lit_cnt; ++i) {
    SymbId id = dict_.Resolve(src[i]);

    CHECK(id != LiteralDictBase::kInvalidId) << src[i];

    LittleEndian::Store16(dest + i, id);
  }
  lit_data_.resize_assume_reserved(lit_cnt * sizeof(SymbId));

  return dict_.alphabet_size();
}


// Decoder
SeqDecoderBase::SeqDecoderBase() {
  data_buf_.reserve(SEQ_BLOCK_SIZE);
}

SeqDecoderBase::~SeqDecoderBase() {}

void SeqDecoderBase::SetDict(const uint8_t* src, unsigned cnt) {
  CHECK_GE(cnt, 6);

  uint32_t lit_dict_sz = LittleEndian::Load24(src);
  uint32_t seq_dict_sz = LittleEndian::Load24(src + 3);
  CHECK_EQ(cnt, lit_dict_sz + seq_dict_sz + 6);
  src += 6;

  SetLitDict(ByteRange(src, lit_dict_sz));

  src += lit_dict_sz;

  if (seq_dict_sz > 0) {
    seq_dict_.SerializeFrom(src, seq_dict_sz);
  }

  seq_dict_range_.clear();
  for (auto it = seq_dict_.begin(); it != seq_dict_.end(); ++it) {
    seq_dict_range_.push_back(*it);
  }
}

int SeqDecoderBase::Decompress(strings::ByteRange br, uint32_t* consumed) {
  *consumed = 0;

  if (!read_header_) {
    if (br.size() < 3) {
      return -6;
    }
    uint16_t magic = LittleEndian::Load16(br.data());
    CHECK_EQ(magic, kMagic);

    size_t hs = BlockHeader::HeaderSize(br[2]) + 2;
    if (br.size() < hs) {
      return -hs;
    }
    bh_.Read(br.data() + 2);
    *consumed = hs;
    br.advance(hs);

    read_header_ = true;
  }

  size_t total_sz = bh_.byte_len_size_comprs + bh_.sequence_size_comprs;
  if (br.size() < total_sz)
    return -total_sz;

  if (bh_.flags & BlockHeader::kDictBit) {
    if (!zstd_cntx_) {
      zstd_cntx_.reset(new Zstd);
      data_buf_.reserve(SEQ_BLOCK_SIZE * 2 + 8); // +8 to allow safe flit parsing.
      code_buf_.reserve(SEQ_BLOCK_SIZE);
    }

    DecompressCodes(br.data());
    br.advance(bh_.byte_len_size_comprs);

    if (zstd_cntx_->start & 2) {
      VLOG(1) << "Decompressing new frame";

      ZSTD_decompressBegin(zstd_cntx_->context);
      zstd_cntx_->start &= 1;
    } else {
      zstd_cntx_->start ^= 1;  // flip the buffer.
    }

    next_flit_ptr_ = data_buf_.data() + zstd_cntx_->offset();
    uint32_t sz;
    while (true) {
      sz = ZSTD_nextSrcSizeToDecompress(zstd_cntx_->context);
      CHECK_LE(sz, br.size());
      if (sz == 0) {
        break;
      }

      size_t res = ZSTD_decompressContinue(zstd_cntx_->context, next_flit_ptr_,
                                           SEQ_BLOCK_SIZE, br.data(), sz);
      CHECK_ZSTDERR(res);

      br.advance(sz);

      if (res > 0) {
        data_buf_.resize_assume_reserved(next_flit_ptr_ + res - data_buf_.data());
        sz = ZSTD_nextSrcSizeToDecompress(zstd_cntx_->context);

        break;
      }

    }
    next_seq_id_ = 0;

    if (sz == 0) {
      zstd_cntx_->start |= 2;
      DCHECK(bh_.flags & BlockHeader::kFinalBit);
    }
  } else {
    size_t res = ZSTD_decompress(data_buf_.data(), data_buf_.capacity(), br.data(),
                                 bh_.sequence_size_comprs);
    CHECK(!ZSTD_isError(res)) << ZSTD_getErrorName(res);
    data_buf_.resize_assume_reserved(res);
  }

  *consumed += total_sz;
  read_header_ = false;
  return (bh_.flags & BlockHeader::kFinalBit) ? 0 : 1;
}

void SeqDecoderBase::DecompressCodes(const uint8_t* src) {
  len_code_.resize(bh_.num_sequences);

  size_t res = ZSTD_decompress(code_buf_.data(), code_buf_.capacity(), src,
                               bh_.byte_len_size_comprs);
  CHECK(!ZSTD_isError(res)) << ZSTD_getErrorName(res);

  uint8_t* next = code_buf_.data(), *end = next + res;
  size_t index = 0;
  while (next < end && index < len_code_.size()) {
    next += flit::ParseT(next, &len_code_[index]);
    index++;
  }
  CHECK_EQ(index, len_code_.size());
  CHECK_EQ(next, end);
}

void SeqDecoderBase::InflateSequences() {
  const uint8_t* end_capacity = data_buf_.data() + data_buf_.capacity();

  for (; next_seq_id_ < len_code_.size(); ++next_seq_id_) {
    uint32_t len_code = len_code_[next_seq_id_];
    uint32_t len = 0;
    bool success = false;
    if (IsDictCode(len_code)) { // Dict
      CHECK(!seq_dict_.empty());

      uint32_t id = len_code >> 1;
      CHECK_LT(id, seq_dict_range_.size());
      const ByteRange& src = seq_dict_range_[id];
      success = AddFlitSeq(src);
    } else {
      len = len_code >> 1;

      CHECK_LE(next_flit_ptr_ + len, end_capacity);
      success = AddFlitSeq(ByteRange(next_flit_ptr_, len));
    }

    if (!success)
      break;

    // Must be after success check.
    next_flit_ptr_ += len;
  }
}


template<size_t INT_SIZE> SeqDecoder<INT_SIZE>::SeqDecoder() {
  int_buf_.reserve(SEQ_BLOCK_SIZE / INT_SIZE);
}

template<size_t INT_SIZE> auto SeqDecoder<INT_SIZE>::GetNextIntPage() -> IntRange {
  if (!(bh_.flags & BlockHeader::kDictBit)) {
    CHECK_EQ(0, data_buf_.size() % INT_SIZE);

    IntRange res(reinterpret_cast<UT*>(data_buf_.data()), data_buf_.size() / INT_SIZE);

    // clear() does not really change the underlying memory and it still belongs to data_buf_.
    // for non-dict encoding it will return an empty page for the next call.
    data_buf_.clear();

    return res;
  }

  next_int_ptr_ = int_buf_.data();

  InflateSequences();

  int_buf_.resize_assume_reserved(next_int_ptr_ - int_buf_.data());

  return IntRange(int_buf_.data(), next_int_ptr_);
}

template<size_t INT_SIZE> void SeqDecoder<INT_SIZE>::SetLitDict(strings::ByteRange br) {
  CHECK_EQ(0, br.size() % INT_SIZE);

  lit_dict_.resize(br.size() / INT_SIZE);
  memcpy(lit_dict_.data(), br.data(), br.size());
}


template<size_t INT_SIZE> bool SeqDecoder<INT_SIZE>::AddFlitSeq(strings::ByteRange src) {
  size_t sz = next_int_ptr_ - int_buf_.begin();
  if (sz >= int_buf_.capacity())
    return false;

  uint32_t left_capacity = int_buf_.capacity() - sz;
  uint32_t expanded =
      internal::DeflateFlitAndMap(
        src.data(), src.size(),
        [this](unsigned id) { return lit_dict_[id];},
        next_int_ptr_, left_capacity);
  if (expanded > left_capacity)
    return false;

  next_int_ptr_ += expanded;

  return true;
}

template class SeqEncoder<4>;
template class SeqEncoder<8>;

template class SeqDecoder<4>;
template class SeqDecoder<8>;

}  // namespace util
