// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <vector>
#include <unordered_map>

#include "base/pod_array.h"
#include "base/flit.h"

#include "strings/unique_strings.h"
#include "util/coding/sequence_array.h"

namespace util {

// src must be sorted. Returns 16bit array of literals (shifted left with lsb 0)
// and rep-codes(lsb 1). The input array should have numbers less than 1 << 15.
// rep-code - means that the previous literal should be replicated another (rep >> 1) times.
// For example, "{1,2,3,4,5,6,7,8,9}" will be translated into 3 numbers:
// "{1, 0 << 1 | 0, (7 << 1) | 1}".
// Encodes inline, i.e. dest and src can point to the same memory region.
// returns number of written literals in the dest.
// *** If src contains 14 bit numbers than it's guaranteed that the flit encoding of output array
// won't be larger than the flit encoding of the input array.
unsigned DeltaEncode16(const uint16_t* src, unsigned cnt, uint16_t* dest);

class LiteralDictBase {
 public:
  typedef uint16 SymbId;

  // To allow varint encoding of at most 2 bytes.
  enum { kMaxAlphabetSize = (1 << 14) - 1, kInvalidId = kMaxAlphabetSize + 1 };

 protected:
  struct Record {
    unsigned cnt;
    SymbId id;

    Record() : cnt(0), id(0) {}
  };
};

template<typename T> class LiteralDict : public LiteralDictBase {
 public:
  typedef T Literal;

  // If true was returned, then arr was recoded into 'cnt' 2-byte Dict::SymbId symbols into dest.
  // False - then the input data does not have a narrow dictionary that justifies its creation,
  // it's too random. dest and arr can be the same pointer to allow recoding in place.
  // bool Prepare(const T* arr, uint32_t cnt, SymbId* dest);

  void Add(T val) { ++freq_map_[val].cnt; }

  size_t alphabet_size() const { return alphabet_.size(); }

  T FromId(SymbId i) const { return alphabet_[i]; }

  // returns kInvalidId if not found.
  SymbId Resolve(T t) const {
    auto it = freq_map_.find(t);
    if (it == std::end(freq_map_))
      return kInvalidId;
    return it->second.id;
  }

  // dest and src can point to the same address to allow recoding in place.
  bool Resolve(const T* src, uint32_t count, SymbId* dest);

  void Clear() {
    freq_map_.clear();
    alphabet_.clear();
  }

  // dest must have at least GetMaxSerializedSize.
  size_t SerializeTo(uint8_t* dest) const;
  size_t GetMaxSerializedSize() const;

  void Build();

  size_t dict_size() const { return freq_map_.size(); }
 private:

  std::unordered_map<T, Record> freq_map_;
  std::vector<T> alphabet_;
};


struct BlockHeader {
  enum { kDictBit = 0x1, kFinalBit = 0x2, kDictSeqBit = 0x4 };

  uint8_t flags;  // use dictionary.

  // num_sequences and byte_len_size_comprs defined only if "use dictionary" is set.
  uint16_t num_sequences;

  // 3 bytes because we do not store more than 128K of intermediate data in a single block.
  uint32_t byte_len_size_comprs;  // 3 bytes
  uint32_t sequence_size_comprs;  // 3 bytes.

  uint8_t Write(uint8_t* dest) const;

  // Reads data from src into the object. must have at least HeaderSize bytes.
  // src should point right after magic string.
  void Read(const uint8_t* src);

  // Not including magic string but including flags byte.
  static uint8_t HeaderSize(uint8_t flags);

  BlockHeader() : flags(0), num_sequences(0), byte_len_size_comprs(0),
    sequence_size_comprs(0) {}
};

class SeqEncoderBase {
 public:
  SeqEncoderBase();

  virtual ~SeqEncoderBase();

  const std::vector<strings::ByteRange>& compressed_blocks() const {
    return compressed_blocks_;
  }

  void ClearCompressedData() {
    compressed_bufs_.clear();
    compressed_blocks_.clear();
  }

  void DisableSeqDictionary() { disable_seq_dict_ = true; }

  uint32 Cost() const;

  void Flush();

 protected:
  using SymbId = LiteralDictBase::SymbId;

  void AnalyzePreDict();

  // Returns true of the sequence was assed to seq_map_. .
  bool LearnSeqDict(strings::ByteRange entry);

  // Compresses flit sequence data into a binary blob.
  // Empties lit_data_  buffer.
  void CompressFlitSequences(bool final);

  // Compress raw literals as is.
  void CompressRawLit(bool final);

  void AddCompressedBuf(const BlockHeader& bh);

  void AnalyzeSequenceDict();

  // Checks if cnt literals can be added to batch buffers.
  // Returns true if succeeded, false if encoder state has been changed.
  bool PrepareForSymbAvailability(uint32_t cnt);

  void BacktrackToRaw();
  void AddEncodedSymbols(SymbId* src, uint32_t cnt);

  virtual uint32_t PrepareDict() = 0;

  base::PODArray<uint8> lit_data_, prev_block_;

  // For sequence dictionary mode, after dict_seq_map_ is built len_ points either to
  // consequetive sequences in frame_seq_
  // (using id = 0) or using index from dict_seq_ (by actually storing index + 1).
  base::PODArray<uint32> len_code_;

  static_assert(alignof(uint32_t) <= decltype(lit_data_)::alignment_v, "");

  std::vector<std::unique_ptr<uint8_t[]>> compressed_bufs_;
  std::vector<strings::ByteRange> compressed_blocks_;

  bool disable_seq_dict_ = false;
  uint32_t literal_size_; // Single literal size (4 or 8 bytes).

  /*
    The sequences are stored fully in each block, no overflows to the next one.
    In case of NO_LIT_DICT we just store "uncompressible data" only literals.
*/
  // NO_LIT_DICT is final and can be switched to from any state. It means that the literals
  // dictionary can not be built or not worth to be built. This can be decide abruptly in the
  // middle of processing so the algorithm should always know how to fallback to that state.
  // PRE_DICT switches to LIT_DICT or to NO_LIT_DICT.
  // LIT_DICT switches to NO_LIT_DICT.
  //
  // PRE_DICT - to gather the first batch and to decide if it's possible to encode literals with
  // dictionary encoding + varint/flit encoding.
  // If yes, switches to LIT_DICT with all sequences reduced using dictionary and flit encodings.
  // They are mapped for building seq dictionary via seq_map_.
  // LIT_DICT - gathers all the sequences via seq_map_ until it's full.
  // LIT_DICT has mini states by its own. It can process seq_map_ and to decide whether dict_seq_
  // should be filled. If dict_seq_ is filled then LIT_DICT uses both literal and sequence
  /// dictionary. In any case lit_data_ and len_ contain binary blobs and len codes to decode them.
  // At this moment sequence dictionary is final once it's built in CreateSequenceDict().
  enum State { PRE_DICT, LIT_DICT, NO_LIT_DICT } state_ = PRE_DICT;

  struct EntryVal {
    uint32 ref_cnt = 0;
    uint32 dict_id = 0;
  };

  // First step map - gathers all the sequences.
  // Allows to store the information about duplicates during the learning phase.
  google::dense_hash_map<strings::ByteRange, EntryVal> seq_map_;
  base::PODArray<uint32_t> duplicate_seq_;

  // dict_seq_ is the extracted dictionary.
  SequenceArray dict_seq_;
  google::dense_hash_map<strings::ByteRange, uint32> dict_seq_map_;  // reverse mapping.

  base::PODArray<uint8> compress_data_, tmp_space_;
  base::PODArray<SymbId> tmp_symb_;
  struct ZstdCntx;

  std::unique_ptr<ZstdCntx> zstd_cntx_;

  std::unique_ptr<uint8_t[]> zstd_dict_;
  size_t zstd_dict_size_ = 0;
  size_t added_lit_cnt_ = 0, dict_ref_bytes_ = 0;
  double dict_nominal_ratio_ = 0;
};

template<size_t INT_SIZE> class SeqEncoder : public SeqEncoderBase  {
  static_assert(INT_SIZE == 4 || INT_SIZE == 8, "");

 public:
  using UT = std::conditional_t<INT_SIZE == 4, uint32_t, uint64_t>;

  SeqEncoder();

  void Add(const UT* src, unsigned cnt);

  // Return false if no dictionary was used.
  bool GetDictSerialized(std::string* dest);

 private:

  // Returns alphabet size.
  uint32_t PrepareDict() override;

  bool AddDictEncoded(const UT* src, unsigned cnt);

  LiteralDict<UT> dict_;
};

class SeqDecoderBase {
  using SymbId = LiteralDictBase::SymbId;
 public:
  SeqDecoderBase();
  virtual ~SeqDecoderBase();

  // Returns 0 if decompression of the frame is ended, 1 if it's still going.
  // In any case "*consumed" will hold how many bytes were consumed from br.
  // If negative number is returned - then last portion of br is too small to decompress
  // In that case, the -(return value) will tell how many input bytes are needed.
  int Decompress(strings::ByteRange br, uint32_t* consumed);

  void SetDict(const uint8_t* src, unsigned cnt);

 protected:
  void InflateSequences();

  void DecompressCodes(const uint8_t* src);

  virtual void SetLitDict(strings::ByteRange br) = 0;
  virtual bool AddFlitSeq(strings::ByteRange src) = 0;

  BlockHeader bh_;
  bool read_header_ = false;

  base::PODArray<uint32_t> len_code_;
  base::PODArray<uint8_t> code_buf_, data_buf_;

  SequenceArray seq_dict_;
  std::vector<strings::ByteRange> seq_dict_range_;

  uint32_t next_seq_id_ = 0;
  uint8_t* next_flit_ptr_;

  struct Zstd;
  std::unique_ptr<Zstd> zstd_cntx_;
};


template<size_t INT_SIZE> class SeqDecoder: public SeqDecoderBase {
  static_assert(INT_SIZE == 4 || INT_SIZE == 8, "");

 public:
  SeqDecoder();

  using UT = std::conditional_t<INT_SIZE == 4, uint32_t, uint64_t>;
  using IntRange = strings::Range<UT*>;

  // After calling Decompress, this function returns the currently available non-empty integer page
  // based on the decompressed block. If no more data is available - returns an empty range.
  /* res = decoder.Decompress(src, &consumed);
     for (auto page = decoder.GetNextIntPage(); !page.empty(); page = decoder.GetNextIntPage()) {
       // *do something with the page
     };
     TODO: this function requires to allocate int_buf_ which is not really needed if we copy
           into a 3rd party buffer. To revisit the interface and to allow copying data
           using the sliding window.
  */
  IntRange GetNextIntPage();

 private:
  void SetLitDict(strings::ByteRange br) override;
  bool AddFlitSeq(strings::ByteRange src) override;

  base::PODArray<UT> lit_dict_, int_buf_;

  UT* next_int_ptr_;
};

namespace internal {
constexpr unsigned kSmallNum = 5;

template<typename UT, typename MapperFn> uint32_t DeflateFlitAndMap(
  const uint8_t* src, uint32_t cnt, MapperFn mapper_fn,
  UT* dest, uint32_t dest_capacity) {
  namespace flit = base::flit;

  const uint8_t* end = src + cnt;
  uint32_t val;
  const uint8_t* next = src + flit::ParseT(src, &val);
  LiteralDictBase::SymbId symbid = val;

  dest[0] = mapper_fn(symbid);

  uint32_t dest_index = 1;
  bool prev_small = val < kSmallNum;
  uint32_t prev_val = val + 1;

  while (next < end) {
    next += flit::ParseT(next, &val);

    if (prev_small) {
      bool is_rep = val & 1;
      val >>= 1;

      if (is_rep) {
        val += 1;

        if (dest_index + val > dest_capacity)
          return dest_index + val;

        for (unsigned i = 0; i < val; ++i) {
          symbid += prev_val;
          dest[dest_index++] = mapper_fn(symbid);
        }
        prev_small = false;
        continue;
      }
    }

    if (dest_index >= dest_capacity)
      return dest_index + 1;

    prev_small = val < kSmallNum;
    ++val;
    symbid += val;
    prev_val = val;

    dest[dest_index++] = mapper_fn(symbid);
  }

  return dest_index;
}

}  // namespace internal

}  // namespace util
