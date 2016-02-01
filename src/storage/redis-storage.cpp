/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/**
 * Copyright (c) 2014,  Regents of the University of California.
 * Copyright (c) 2015, Washington University in St. Louis
 *
 * This file is part of NDN repo-ng (Next generation of NDN repository).
 * See AUTHORS.md for complete list of repo-ng authors and contributors.
 *
 * repo-ng is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * repo-ng is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * repo-ng, e.g., in COPYING.md file.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "../../build/src/config.hpp"
#include "redis-storage.hpp"
#include "index.hpp"
#include <boost/filesystem.hpp>
#include <istream>

namespace repo {

using std::string;
using std::cout;
using std::endl;

RedisStorage::RedisStorage(const string& dbPath)
  : m_size(0)
{
  cout << "RedisStorage constructor\n";

  m_host = "192.168.1.2"; // Redis IP address
  // m_host = "127.0.0.1";
  m_size = 0;
  m_id = 1; // Simply record the next available insertion ID
  initializeRepo();
}

void
RedisStorage::initializeRepo()
{
  cout << "initializeRepo\n";

  int port = 6379;
  struct timeval timeout = { 1, 500000 }; // 1.5 seconds
  m_c = redisConnectWithTimeout(m_host.c_str(), port, timeout);
  //m_c = redisConnectUnix("/tmp/redis.sock"); // if Unix socket is used

  if (m_c == NULL || m_c->err) {
      if (m_c) {
          printf("Connection error: %s\n", m_c->errstr);
          redisFree(m_c);
      } else {
          printf("Connection error: can't allocate redis context\n");
      }
      exit(1);
  }

  redisReply * reply = (redisReply *)(redisCommand(m_c,"SET id 1"));
  printf("SET (binary API): %s\n", reply->str);
  freeReplyObject(reply);
}

RedisStorage::~RedisStorage()
{
  cout << "RedisStorage destructor\n";
  redisFree(m_c);
}

// We don't support enumerating all the items at the moment
void
RedisStorage::fullEnumerate(const ndn::function
                             <void(const Storage::ItemMeta)>& f)
{
  cout << "fullEnumerate\n";
}

// Insert data.wireEncode().wire()
// Also maintain a counter in redis
// The data in Redis is in the form of <id, data> or <name, data>
// id is looked up using the in-memory indexing structure
int64_t
RedisStorage::insert(const Data& data)
{
  // cout << "insert\n";
  Name name = data.getName();
  // cout << "name = " << name << endl;
  Index::Entry entry(data, 0); //the id is not used
  // int64_t id = -1;
  if (name.empty()) {
    std::cerr << "name is empty" << std::endl;
    return -1;
  }

  /* An alternative way of getting the next available ID
  redisCommand(m_c, "INCR id");
  redisReply *  id_reply = (redisReply *)(redisCommand(m_c, "GET id"));
  printf("new id = %s\n", id_reply->str);
  */
#if 0
  // Data form <id, data>
  string id = std::to_string(m_id);
#else
  // Data form <name, data>
  string id = name.toUri();
#endif
  redisReply * reply = (redisReply *)(redisCommand(m_c,"SET %b %b", id.c_str(), strlen(id.c_str()), (data.wireEncode().wire()), data.wireEncode().size()));

  freeReplyObject(reply);
  // freeReplyObject(id_reply);

  return m_id++;
}


// erase is currently not supported
bool
RedisStorage::erase(const int64_t id)
{
  // cout << "erase\n";
  return true;
}

// Given an id, return the data packet
shared_ptr<Data>
RedisStorage::read(const int64_t id)
{
  shared_ptr<Data> data(new Data());

  string lookup_id = std::to_string(id);
  redisReply * reply = (redisReply *)(redisCommand(m_c,"GET %b", lookup_id.c_str(), strlen(lookup_id.c_str())) );

  data->wireDecode(Block(reply->str, reply->len));
  freeReplyObject(reply);

  return data;
}

// Given an Interest, return the data packet
shared_ptr<Data>
RedisStorage::read(const Interest& interest)
{
  shared_ptr<Data> data(new Data());

  Name name = interest.getName();
  string lookup_id = name.toUri();
  redisReply * reply = (redisReply *)(redisCommand(m_c,"GET %b", lookup_id.c_str(), strlen(lookup_id.c_str())) );

  // printf("GET: %s\n", reply->str);
  // std::cout << "length = " << reply->len << std::endl;

	if (reply->len) {
  	data->wireDecode(Block(reply->str, reply->len));
  	freeReplyObject(reply);

  	return data;
	}

  // std::cout << "read data name = " << data->getName() << std::endl;

  return shared_ptr<Data>();
}

// resize() is currently not supported
int64_t
RedisStorage::size()
{
  cout << "size\n";
  return 0;
}

} //namespace repo
