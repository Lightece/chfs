#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {
  void output_block_info(BlockInfo block_info){
    std::cout << "block_id:" << std::get<0>(block_info) << ", mac_id:" << std::get<1>(block_info) << ", version:"<< std::get<2>(block_info)<<  std::endl;
  }
  void output_bytes(std::vector<u8> const &bytes){
    std::cout << "bytes: ";
    for(auto byte: bytes){
      std::cout << std::hex << (int)byte << " ";
    }
    std::cout << std::endl;
  }



  inline auto MetadataServer::bind_handlers() {
    server_->bind("mknode",
                  [this](u8 type, inode_id_t parent, std::string const &name) {
                    return this->mknode(type, parent, name);
                  });
    server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
      return this->unlink(parent, name);
    });
    server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
      return this->lookup(parent, name);
    });
    server_->bind("get_block_map",
                  [this](inode_id_t id) { return this->get_block_map(id); });
    server_->bind("alloc_block",
                  [this](inode_id_t id) { return this->allocate_block(id); });
    server_->bind("free_block",
                  [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                    return this->free_block(id, block, machine_id);
                  });
    server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
    server_->bind("get_type_attr",
                  [this](inode_id_t id) { return this->get_type_attr(id); });
  }

  inline auto MetadataServer::init_fs(const std::string &data_path) {
    /**
     * Check whether the metadata exists or not.
     * If exists, we wouldn't create one from scratch.
     */
    bool is_initialed = is_file_exist(data_path);

    auto block_manager = std::shared_ptr<BlockManager>(nullptr);
    if (is_log_enabled_) {
      block_manager =
          std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
    } else {
      block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
    }

    CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

    if (is_initialed) {
      auto origin_res = FileOperation::create_from_raw(block_manager);
      std::cout << "Restarting..." << std::endl;
      if (origin_res.is_err()) {
        std::cerr << "Original FS is bad, please remove files manually."
                  << std::endl;
        exit(1);
      }

      operation_ = origin_res.unwrap();
    } else {
      operation_ = std::make_shared<FileOperation>(block_manager,
                                                   DistributedMaxInodeSupported);
      std::cout << "We should init one new FS..." << std::endl;
      /**
       * If the filesystem on metadata server is not initialized, create
       * a root directory.
       */
      auto init_res = operation_->alloc_inode(InodeType::Directory);
      if (init_res.is_err()) {
        std::cerr << "Cannot allocate inode for root directory." << std::endl;
        exit(1);
      }

      CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
    }

    running = false;
    num_data_servers =
        0; // Default no data server. Need to call `reg_server` to add.

    if (is_log_enabled_) {
      if (may_failed_)
        operation_->block_manager_->set_may_fail(true);
      commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                               is_checkpoint_enabled_);
    }

    bind_handlers();

    /**
     * The metadata server wouldn't start immediately after construction.
     * It should be launched after all the data servers are registered.
     */
  }

  MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                                 bool is_log_enabled, bool is_checkpoint_enabled,
                                 bool may_failed)
      : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
        is_checkpoint_enabled_(is_checkpoint_enabled) {
    server_ = std::make_unique<RpcServer>(port);
    init_fs(data_path);
    if (is_log_enabled_) {
      commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                               is_checkpoint_enabled);
    }
  }

  MetadataServer::MetadataServer(std::string const &address, u16 port,
                                 const std::string &data_path,
                                 bool is_log_enabled, bool is_checkpoint_enabled,
                                 bool may_failed)
      : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
        is_checkpoint_enabled_(is_checkpoint_enabled) {
    server_ = std::make_unique<RpcServer>(address, port);
    init_fs(data_path);
    if (is_log_enabled_) {
      commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                               is_checkpoint_enabled);
    }
  }

// {Your code here}
  auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
  -> inode_id_t {
    // TODO: Implement this function.
    std::cout << "metadata_server/mknode: " << type << ", " << parent << ", " << name << std::endl;
    auto make_res = this->operation_->mk_helper(parent, name.c_str(), static_cast<InodeType>(type));
    if (make_res.is_err()) {
      return 0;
    }
    std::cout << "metadata_server/mknode finished: inode id=" << make_res.unwrap() << std::endl;
    return make_res.unwrap();
  }

// {Your code here}
  auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
  -> bool {
    // TODO: Implement this function.
    std::cout << "metadata_server/unlink: " << parent << ", " << name << std::endl;
    auto unlink_res = this->operation_->unlink(parent, name.c_str());
    if (unlink_res.is_err()) {
      return false;
    }
    std::cout << "metadata_server/unlink finished!" << std::endl;
    return true;
  }

// {Your code here}
  auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
  -> inode_id_t {
    // TODO: Implement this function.
    std::cout << "metadata_server/lookup: " << parent << ", " << name << std::endl;
    auto lookup_res = this->operation_->lookup(parent, name.c_str());
    if (lookup_res.is_err()) {
      return 0;
    }
    return lookup_res.unwrap();
  }

// {Your code here}
  auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
    // TODO: Implement this function.
    std::cout << "metadata_server/get_block_map: " << id << std::endl;
    // 1 找到当前inode存储的block id
    auto read_inode_res = this->operation_->inode_manager_->get(id);
    if(read_inode_res.is_err()) return {};
    auto inode_block_id = read_inode_res.unwrap();
    // 2 读取inode block
    std::vector<u8> inode_block(DiskBlockSize);
    auto read_block_res = this->operation_->block_manager_->read_block(inode_block_id, inode_block.data());
    if(read_block_res.is_err()) return {};
    // 3 跳过Inode信息，读取block info
    std::vector<u8> buffer = std::vector<u8>(inode_block.begin()+sizeof(Inode), inode_block.end());
//    output_bytes(buffer);
    std::vector<BlockInfo> block_info_list;
    auto inode = (Inode*) inode_block.data();
    auto nBlocks = inode->get_nblocks();
    std::cout << "nBlocks:" << nBlocks << std::endl;
    auto blocks = (BlockInfo*) buffer.data();
    std::cout << "nBlocks:" << nBlocks << std::endl;
    for(int i = 0; i < nBlocks; i++){
      if(std::get<0>(blocks[i]) !=KInvalidBlockID){
//        output_block_info(blocks[i]);
        block_info_list.emplace_back(blocks[i]);
      }
    }
    std::cout<< "metadata_server/get_block_map: block_info_list, size=" << block_info_list.size() << std::endl;
    return block_info_list;
  }

// {Your code here}
  auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
    // TODO: Implement this function.
    std::cout << "metadata_server/allocate_block: " << id << std::endl;
    mac_id_t mac_id = this->generator.rand(1, num_data_servers);
    auto callResponse = clients_[mac_id]->call("alloc_block");
    auto response = callResponse.unwrap()->as<std::pair<block_id_t, version_t>>();
    BlockInfo block_info = {response.first, mac_id, response.second};
//    output_block_info(block_info);
    // 获取当前inode的blocks列表
    std::vector<BlockInfo> blocks = this->get_block_map(id);
    // no more empty blocks
    if(blocks.size()>= (DiskBlockSize - sizeof(Inode))/sizeof(BlockInfo))  return {};
    // write back in inode
    blocks.push_back(block_info);
    auto get_inode_res = this->operation_->inode_manager_->get(id);
    if(get_inode_res.is_err()) return {};
    block_id_t inode_block_id = get_inode_res.unwrap();
    auto write_res = this->operation_->block_manager_->write_partial_block(inode_block_id, (u8*)blocks.data(),  sizeof(Inode), blocks.size()*sizeof(BlockInfo));
    if(write_res.is_err()) return {};
    return block_info;
  }

// {Your code here}
  auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                  mac_id_t machine_id) -> bool {
    // TODO: Implement this function.
    std::cout << "metadata_server/free_block: " << id << ", " << block_id << ", " << machine_id << std::endl;
    auto read_inode_res = this->operation_->read_file(id);
    if(read_inode_res.is_err()) return false;
    auto inode_block = read_inode_res.unwrap();
    auto block_info_num = DiskBlockSize/sizeof(BlockInfo);
    bool found_curr = false;
    for(int i=0; i<block_info_num; i++){
      BlockInfo curr = *((BlockInfo*)inode_block.data()+i);
      if(std::get<block_id_t>(curr) == block_id){
        *((BlockInfo*)inode_block.data()+i) = {KInvalidBlockID, 0, 0};
        found_curr = true;
        break;
      }
    }
    if(!found_curr) return false;
    auto free_res = this->operation_->block_allocator_->deallocate(block_id);
    if(free_res.is_err()) return false;
    return true;
  }

// {Your code here}
  auto MetadataServer::readdir(inode_id_t node)
  -> std::vector<std::pair<std::string, inode_id_t>> {
    // TODO: Implement this function.
    std::cout << "metadata_server/readdir: " << node << std::endl;
    auto read_inode_res = this->operation_->read_file(node);
    if(read_inode_res.is_err()) return {};
    auto dir_content = read_inode_res.unwrap();
    std::vector<std::pair<std::string, inode_id_t>> dir_list;
    std::string dir_list_str(dir_content.begin(), dir_content.end());
    while (dir_list_str.length() > 0) {
      auto pos = dir_list_str.find('/');
      auto entry = dir_list_str.substr(0, pos);
      auto pos2 = entry.find(':');
      auto name = entry.substr(0, pos2);
      std::string id_str = entry.substr(pos2 + 1);
      std::stringstream ss(id_str);
      inode_id_t tmp_inode_id;
      ss >> tmp_inode_id;
      dir_list.emplace_back(name, tmp_inode_id);
//      std::cout << "metadata_server/readdir: name=" << name << ", id=" << tmp_inode_id << std::endl;
      if (pos == std::string::npos) {
        break;
      }
      dir_list_str = dir_list_str.substr(pos + 1);
    }
    return dir_list;
  }

// {Your code here}
  auto MetadataServer::get_type_attr(inode_id_t id)
  -> std::tuple<u64, u64, u64, u64, u8> {
    // TODO: Implement this function.
    std::cout << "metadata_server/get_type_attr: " << id << std::endl;
    auto read_inode_res = this->operation_->read_file(id);
    if(read_inode_res.is_err()) return {};
    auto inode_block = read_inode_res.unwrap();
    auto inode = (Inode*) inode_block.data();
    auto attr = inode->get_attr();
    return {attr.size, attr.atime, attr.mtime, attr.ctime, static_cast<const unsigned char>(inode->get_type())};
  }

  auto MetadataServer::reg_server(const std::string &address, u16 port,
                                  bool reliable) -> bool {
    num_data_servers += 1;
    auto cli = std::make_shared<RpcClient>(address, port, reliable);
    clients_.insert(std::make_pair(num_data_servers, cli));

    return true;
  }

  auto MetadataServer::run() -> bool {
    if (running)
      return false;

    // Currently we only support async start
    server_->run(true, num_worker_threads);
    running = true;
    return true;
  }

} // namespace chfs