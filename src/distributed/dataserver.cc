#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

  auto DataServer::initialize(std::string const &data_path) {
    /**
     * At first check whether the file exists or not.
     * If so, which means the distributed chfs has
     * already been initialized and can be rebuilt from
     * existing data.
     */
    bool is_initialized = is_file_exist(data_path);

    auto bm = std::shared_ptr<BlockManager>(
        new BlockManager(data_path, KDefaultBlockCnt));
    if (is_initialized) {
      block_allocator_ =
          std::make_shared<BlockAllocator>(bm, 0, false);
    } else {
      // We need to reserve some blocks for storing the version of each block
      block_allocator_ = std::shared_ptr<BlockAllocator>(
          new BlockAllocator(bm, 0, true));
    }

    // Initialize the RPC server and bind all handlers
    server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                      usize len, version_t version) {
      return this->read_data(block_id, offset, len, version);
    });
    server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                       std::vector<u8> &buffer) {
      return this->write_data(block_id, offset, buffer);
    });
    server_->bind("alloc_block", [this]() { return this->alloc_block(); });
    server_->bind("free_block", [this](block_id_t block_id) {
      return this->free_block(block_id);
    });

    // Launch the rpc server to listen for requests
    server_->run(true, num_worker_threads);
  }

  DataServer::DataServer(u16 port, const std::string &data_path)
      : server_(std::make_unique<RpcServer>(port)) {
    initialize(data_path);
  }

  DataServer::DataServer(std::string const &address, u16 port,
                         const std::string &data_path)
      : server_(std::make_unique<RpcServer>(address, port)) {
    initialize(data_path);
  }

  DataServer::~DataServer() { server_.reset(); }

// {Your code here}
  auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                             version_t version) -> std::vector<u8> {
    // TODO: Implement this function.
//  UNIMPLEMENTED();
    // if version mismatch, reject the request
    std::vector<u8> buffer(this->block_allocator_->bm->block_size());
    auto version_block_idx = block_id/(this->block_allocator_->bm->block_size()/sizeof(version_t));
    auto version_block_offset = block_id%(this->block_allocator_->bm->block_size()/sizeof(version_t));
    auto read_version_res = this->block_allocator_->bm->read_block(version_block_idx+1, buffer.data());
//    auto error_code = ErrorType::DONE;
    if(read_version_res.is_err()){
//      error_code = read_version_res.unwrap_error();
      return {};
    }
    auto local_version = *(reinterpret_cast<version_t*>(buffer.data())+version_block_offset);
    if(local_version != version){
      return {};
    }
    auto read_res = this->block_allocator_->bm->read_block(block_id, buffer.data());
    if (read_res.is_err()){
//      error_code = read_res.unwrap_error();
      return {};
    }
    return std::vector<u8>(buffer.begin()+offset, buffer.begin()+offset+len);
  }

// {Your code here}
  auto DataServer::write_data(block_id_t block_id, usize offset,
                              std::vector<u8> &buffer) -> bool {
    // TODO: Implement this function.
    auto write_res = this->block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());
    if(write_res.is_err()){
      return false;
    }
    return true;
  }

// {Your code here}
  auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
    // TODO: Implement this function.
//    UNIMPLEMENTED();
    auto alloc_res = this->block_allocator_->allocate();
    if(alloc_res.is_err()){
      return {};
    }
    auto block_id = alloc_res.unwrap();
    auto version_block_idx = block_id/(this->block_allocator_->bm->block_size()/sizeof(version_t));
    auto version_block_offset = block_id%(this->block_allocator_->bm->block_size()/sizeof(version_t));
    std::vector<u8> buffer(this->block_allocator_->bm->block_size());
    auto read_version_res = this->block_allocator_->bm->read_block(version_block_idx+1, buffer.data());
    if(read_version_res.is_err()){
      return {};
    }
    auto new_version = *(reinterpret_cast<version_t*>(buffer.data())+version_block_offset) + 1;
    // write back version+1
    auto write_version_res = this->block_allocator_->bm->write_partial_block(version_block_idx+1, reinterpret_cast<u8*>(&new_version), version_block_offset*sizeof(version_t), sizeof(version_t));
    if(write_version_res.is_err()){
      return {};
    }
    return {block_id, new_version};
  }

// {Your code here}
  auto DataServer::free_block(block_id_t block_id) -> bool {
    // TODO: Implement this function.
//    UNIMPLEMENTED();
    auto free_res = this->block_allocator_->deallocate(block_id);
    if(free_res.is_err()){
      return false;
    }
    auto version_block_idx = block_id/(this->block_allocator_->bm->block_size()/sizeof(version_t));
    auto version_block_offset = block_id%(this->block_allocator_->bm->block_size()/sizeof(version_t));
    std::vector<u8> buffer(this->block_allocator_->bm->block_size());
    auto read_version_res = this->block_allocator_->bm->read_block(version_block_idx+1, buffer.data());
    if(read_version_res.is_err()){
      return false;
    }
    auto new_version = *(reinterpret_cast<version_t*>(buffer.data())+version_block_offset) + 1;
    // write back version+1
    auto write_version_res = this->block_allocator_->bm->write_partial_block(version_block_idx+1, reinterpret_cast<u8*>(&new_version), version_block_offset*sizeof(version_t), sizeof(version_t));
    if(write_version_res.is_err()){
      return false;
    }
    return true;
  }
} // namespace chfs