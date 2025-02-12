// Copyright (c) 2009 Satoshi Nakamoto
// Distributed under the MIT/X11 software license, see the accompanying
// file license.txt or http://www.opensource.org/licenses/mit-license.php.

#include "headers.h"






//
// CDB
//

// A lock on cs_db is needed to make changes to the variables directly
// below (fDbEnvInit, dbenv, and mapFileUseCount). Since these variables
// are only used in this file, they have `static` prepended to indicate
// that other files shouldn;t be able to access them.
static CCriticalSection cs_db;
// Indicates whether the DB environment has been initiated:
static bool fDbEnvInit = false;
DbEnv dbenv(0);
// A key-value mapping indicating how many times a file is
// used when opening a DB connection.
static map<string, int> mapFileUseCount;

class CDBInit
{
public:
    // Constructor:
    CDBInit()
    {
    }
    // Destructor (guaranteed to be called when the object goes out of scope):
    ~CDBInit()
    {
        if (fDbEnvInit)
        {
            dbenv.close(0);
            fDbEnvInit = false;
        }
    }
}
// We place the instance of CDBInit in a global variable so that
// it will be destructed when it goes out of scope.
instance_of_cdbinit;

/**
 * Constructor that creates a new instance of CDB.
 */
CDB::CDB(const char* pszFile, const char* pszMode, bool fTxn) : pdb(NULL)
{
    // This value will be used to indicate the success (0) or failure (>0)
    // of our attempts to open the 1) DB environment, and 2) the DB connection.
    int ret;
    // If no file given to read to/write from, error:
    if (pszFile == NULL)
        return;

    // Indicates whether this database should be created (if necessary).
    // If the DB file doesn't exist on disk, this flag must be provided
    // or else there will be an error.
    bool fCreate = strchr(pszMode, 'c');
    // Indicates whether this database is being opened in read mode and NOT in write mode:
    bool fReadOnly = (!strchr(pszMode, '+') && !strchr(pszMode, 'w'));
    unsigned int nFlags = DB_THREAD;
    if (fCreate)
        nFlags |= DB_CREATE;
    else if (fReadOnly)
        nFlags |= DB_RDONLY;

    // If specified, wrap operations in a transaction:
    if (!fReadOnly || fTxn)
        nFlags |= DB_AUTO_COMMIT;

    /**
     * Acquire a lock before proceeding.
     */
    CRITICAL_BLOCK(cs_db)
    {
        if (!fDbEnvInit)
        {
            // Make the DB folder (if it doesn't exist):
            string strAppDir = GetAppDir();
            string strLogDir = strAppDir + "\\database";
            _mkdir(strLogDir.c_str());
            printf("dbenv.open strAppDir=%s\n", strAppDir.c_str());

            // Set where DB logs should go:
            dbenv.set_lg_dir(strLogDir.c_str());
            // Set the max log size of a file (in bytes):
            dbenv.set_lg_max(10000000);
            // Set the max number of locks for the DB to allow:
            dbenv.set_lk_max_locks(10000);
            // Set the max number of locked objects for the DB to allow:
            dbenv.set_lk_max_objects(10000);
            dbenv.set_errfile(fopen("db.log", "a")); /// debug
            ///dbenv.log_set_config(DB_LOG_AUTO_REMOVE, 1); /// causes corruption

            // Attempt to open the Berkeley DB environment to use and update ret
            // with the success or failure status of the command.
            ret = dbenv.open(strAppDir.c_str(),
                             DB_CREATE     |
                             DB_INIT_LOCK  |
                             DB_INIT_LOG   |
                             DB_INIT_MPOOL |
                             DB_INIT_TXN   |
                             DB_THREAD     |
                             DB_PRIVATE    |
                             DB_RECOVER,
                             0);

            // If an error when opening the DB environment, throw an error:
            if (ret > 0)
                throw runtime_error(strprintf("CDB() : error %d opening database environment\n", ret));
            // Otherwise, flip the flag so we don't try to do this logic
            // again while the environment is open:
            fDbEnvInit = true;
        }
        // Set CDB's property for the filename to store/read data from:
        strFile = pszFile;
        ++mapFileUseCount[strFile];
    }

    // Make a new instance of the DB
    pdb = new Db(&dbenv, 0);

    // Open the connection and return the success/failure status of the operation:
    ret = pdb->open(NULL,      // Txn pointer
                    pszFile,   // Filename
                    "main",    // Logical db name
                    DB_BTREE,  // Database type
                    nFlags,    // Flags
                    0);

    // The DB->open() method returns a non-zero error value on failure and 0 on success
    if (ret > 0)
    {
        delete pdb;
        pdb = NULL;
        CRITICAL_BLOCK(cs_db)
            --mapFileUseCount[strFile];
        strFile = "";
        throw runtime_error(strprintf("CDB() : can't open database file %s, error %d\n", pszFile, ret));
    }

    if (fCreate && !Exists(string("version")))
        WriteVersion(VERSION);

    RandAddSeed();
}

void CDB::Close()
{
    if (!pdb)
        return;
    if (!vTxn.empty())
        vTxn.front()->abort();
    vTxn.clear();
    pdb->close(0);
    delete pdb;
    pdb = NULL;
    dbenv.txn_checkpoint(0, 0, 0);

    CRITICAL_BLOCK(cs_db)
        --mapFileUseCount[strFile];

    RandAddSeed();
}

void DBFlush(bool fShutdown)
{
    // Flush log data to the actual data file
    //  on all files that are not in use
    printf("DBFlush(%s)\n", fShutdown ? "true" : "false");
    CRITICAL_BLOCK(cs_db)
    {
        dbenv.txn_checkpoint(0, 0, 0);
        map<string, int>::iterator mi = mapFileUseCount.begin();
        while (mi != mapFileUseCount.end())
        {
            string strFile = (*mi).first;
            int nRefCount = (*mi).second;
            if (nRefCount == 0)
            {
                dbenv.lsn_reset(strFile.c_str(), 0);
                mapFileUseCount.erase(mi++);
            }
            else
                mi++;
        }
        if (fShutdown)
        {
            char** listp;
            if (mapFileUseCount.empty())
                dbenv.log_archive(&listp, DB_ARCH_REMOVE);
            dbenv.close(0);
            fDbEnvInit = false;
        }
    }
}






//
// CTxDB
//

bool CTxDB::ReadTxIndex(uint256 hash, CTxIndex& txindex)
{
    assert(!fClient);
    txindex.SetNull();
    return Read(make_pair(string("tx"), hash), txindex);
}

bool CTxDB::UpdateTxIndex(uint256 hash, const CTxIndex& txindex)
{
    assert(!fClient);
    return Write(make_pair(string("tx"), hash), txindex);
}

bool CTxDB::AddTxIndex(const CTransaction& tx, const CDiskTxPos& pos, int nHeight)
{
    assert(!fClient);

    // Add to tx index
    uint256 hash = tx.GetHash();
    CTxIndex txindex(pos, tx.vout.size());
    return Write(make_pair(string("tx"), hash), txindex);
}

bool CTxDB::EraseTxIndex(const CTransaction& tx)
{
    assert(!fClient);
    uint256 hash = tx.GetHash();

    return Erase(make_pair(string("tx"), hash));
}

bool CTxDB::ContainsTx(uint256 hash)
{
    assert(!fClient);
    return Exists(make_pair(string("tx"), hash));
}

bool CTxDB::ReadOwnerTxes(uint160 hash160, int nMinHeight, vector<CTransaction>& vtx)
{
    assert(!fClient);
    vtx.clear();

    // Get cursor
    Dbc* pcursor = GetCursor();
    if (!pcursor)
        return false;

    unsigned int fFlags = DB_SET_RANGE;
    loop
    {
        // Read next record
        CDataStream ssKey;
        if (fFlags == DB_SET_RANGE)
            ssKey << string("owner") << hash160 << CDiskTxPos(0, 0, 0);
        CDataStream ssValue;
        int ret = ReadAtCursor(pcursor, ssKey, ssValue, fFlags);
        fFlags = DB_NEXT;
        if (ret == DB_NOTFOUND)
            break;
        else if (ret != 0)
            return false;

        // Unserialize
        string strType;
        uint160 hashItem;
        CDiskTxPos pos;
        ssKey >> strType >> hashItem >> pos;
        int nItemHeight;
        ssValue >> nItemHeight;

        // Read transaction
        if (strType != "owner" || hashItem != hash160)
            break;
        if (nItemHeight >= nMinHeight)
        {
            vtx.resize(vtx.size()+1);
            if (!vtx.back().ReadFromDisk(pos))
                return false;
        }
    }
    return true;
}

bool CTxDB::ReadDiskTx(uint256 hash, CTransaction& tx, CTxIndex& txindex)
{
    assert(!fClient);
    tx.SetNull();
    if (!ReadTxIndex(hash, txindex))
        return false;
    return (tx.ReadFromDisk(txindex.pos));
}

bool CTxDB::ReadDiskTx(uint256 hash, CTransaction& tx)
{
    CTxIndex txindex;
    return ReadDiskTx(hash, tx, txindex);
}

bool CTxDB::ReadDiskTx(COutPoint outpoint, CTransaction& tx, CTxIndex& txindex)
{
    return ReadDiskTx(outpoint.hash, tx, txindex);
}

bool CTxDB::ReadDiskTx(COutPoint outpoint, CTransaction& tx)
{
    CTxIndex txindex;
    return ReadDiskTx(outpoint.hash, tx, txindex);
}

bool CTxDB::WriteBlockIndex(const CDiskBlockIndex& blockindex)
{
    return Write(make_pair(string("blockindex"), blockindex.GetBlockHash()), blockindex);
}

bool CTxDB::EraseBlockIndex(uint256 hash)
{
    return Erase(make_pair(string("blockindex"), hash));
}

bool CTxDB::ReadHashBestChain(uint256& hashBestChain)
{
    return Read(string("hashBestChain"), hashBestChain);
}

bool CTxDB::WriteHashBestChain(uint256 hashBestChain)
{
    return Write(string("hashBestChain"), hashBestChain);
}

CBlockIndex* InsertBlockIndex(uint256 hash)
{
    if (hash == 0)
        return NULL;

    // Return existing
    map<uint256, CBlockIndex*>::iterator mi = mapBlockIndex.find(hash);
    // If we find a block index with this hash, it's
    // already inserted, so return it without inserting:
    if (mi != mapBlockIndex.end())
        return (*mi).second;

    // Create new
    CBlockIndex* pindexNew = new CBlockIndex();
    if (!pindexNew)
        throw runtime_error("LoadBlockIndex() : new CBlockIndex failed");
    mi = mapBlockIndex.insert(make_pair(hash, pindexNew)).first;
    pindexNew->phashBlock = &((*mi).first);

    return pindexNew;
}

bool CTxDB::LoadBlockIndex()
{
    // Get cursor
    Dbc* pcursor = GetCursor();
    if (!pcursor)
        return false;

    unsigned int fFlags = DB_SET_RANGE;
    loop
    {
        // Read next record
        CDataStream ssKey;
        if (fFlags == DB_SET_RANGE)
            ssKey << make_pair(string("blockindex"), uint256(0));
        CDataStream ssValue;
        int ret = ReadAtCursor(pcursor, ssKey, ssValue, fFlags);
        fFlags = DB_NEXT;
        if (ret == DB_NOTFOUND)
            break;
        else if (ret != 0)
            return false;

        // Unserialize
        string strType;
        // Since the key is a pair, deserialize just the first part:
        ssKey >> strType;
        if (strType == "blockindex")
        {
            CDiskBlockIndex diskindex;
            ssValue >> diskindex;

            // Construct block index object
            CBlockIndex* pindexNew = InsertBlockIndex(diskindex.GetBlockHash());
            pindexNew->pprev          = InsertBlockIndex(diskindex.hashPrev);
            pindexNew->pnext          = InsertBlockIndex(diskindex.hashNext);
            pindexNew->nFile          = diskindex.nFile;
            pindexNew->nBlockPos      = diskindex.nBlockPos;
            pindexNew->nHeight        = diskindex.nHeight;
            pindexNew->nVersion       = diskindex.nVersion;
            pindexNew->hashMerkleRoot = diskindex.hashMerkleRoot;
            pindexNew->nTime          = diskindex.nTime;
            pindexNew->nBits          = diskindex.nBits;
            pindexNew->nNonce         = diskindex.nNonce;

            // Watch for genesis block and best block
            if (pindexGenesisBlock == NULL && diskindex.GetBlockHash() == hashGenesisBlock)
                pindexGenesisBlock = pindexNew;
        }
        else
        {
            break;
        }
    }

    if (!ReadHashBestChain(hashBestChain))
    {
        if (pindexGenesisBlock == NULL)
            return true;
        return error("CTxDB::LoadBlockIndex() : hashBestChain not found\n");
    }

    if (!mapBlockIndex.count(hashBestChain))
        return error("CTxDB::LoadBlockIndex() : blockindex for hashBestChain not found\n");
    pindexBest = mapBlockIndex[hashBestChain];
    nBestHeight = pindexBest->nHeight;
    printf("LoadBlockIndex(): hashBestChain=%s  height=%d\n", hashBestChain.ToString().substr(0,14).c_str(), nBestHeight);

    return true;
}





//
// CAddrDB
//

bool CAddrDB::WriteAddress(const CAddress& addr)
{
    return Write(make_pair(string("addr"), addr.GetKey()), addr);
}

bool CAddrDB::LoadAddresses()
{
    CRITICAL_BLOCK(cs_mapIRCAddresses)
    CRITICAL_BLOCK(cs_mapAddresses)
    {
        // Load user provided addresses
        CAutoFile filein = fopen("addr.txt", "rt");
        if (filein)
        {
            try
            {
                char psz[1000];
                while (fgets(psz, sizeof(psz), filein))
                {
                    CAddress addr(psz, NODE_NETWORK);
                    if (addr.ip != 0)
                    {
                        AddAddress(*this, addr);
                        mapIRCAddresses.insert(make_pair(addr.GetKey(), addr));
                    }
                }
            }
            catch (...) { }
        }

        // Get cursor
        Dbc* pcursor = GetCursor();
        if (!pcursor)
            return false;

        loop
        {
            // Read next record
            CDataStream ssKey;
            CDataStream ssValue;
            int ret = ReadAtCursor(pcursor, ssKey, ssValue);
            if (ret == DB_NOTFOUND)
                break;
            else if (ret != 0)
                return false;

            // Unserialize
            string strType;
            ssKey >> strType;
            if (strType == "addr")
            {
                CAddress addr;
                ssValue >> addr;
                mapAddresses.insert(make_pair(addr.GetKey(), addr));
            }
        }

        //// debug print
        printf("mapAddresses:\n");
        foreach(const PAIRTYPE(vector<unsigned char>, CAddress)& item, mapAddresses)
            item.second.print();
        printf("-----\n");

        // Fix for possible bug that manifests in mapAddresses.count in irc.cpp,
        // just need to call count here and it doesn't happen there.  The bug was the
        // pack pragma in irc.cpp and has been fixed, but I'm not in a hurry to delete this.
        mapAddresses.count(vector<unsigned char>(18));
    }

    return true;
}

bool LoadAddresses()
{
    return CAddrDB("cr+").LoadAddresses();
}




//
// CReviewDB
//

bool CReviewDB::ReadReviews(uint256 hash, vector<CReview>& vReviews)
{
    vReviews.size(); // msvc workaround, just need to do anything with vReviews
    return Read(make_pair(string("reviews"), hash), vReviews);
}

bool CReviewDB::WriteReviews(uint256 hash, const vector<CReview>& vReviews)
{
    return Write(make_pair(string("reviews"), hash), vReviews);
}







//
// CWalletDB
//

bool CWalletDB::LoadWallet(vector<unsigned char>& vchDefaultKeyRet)
{
    vchDefaultKeyRet.clear();

    //// todo: shouldn't we catch exceptions and try to recover and continue?
    CRITICAL_BLOCK(cs_mapKeys)
    CRITICAL_BLOCK(cs_mapWallet)
    {
        // Get cursor
        Dbc* pcursor = GetCursor();
        if (!pcursor)
            return false;

        loop
        {
            // Read next record
            CDataStream ssKey;
            CDataStream ssValue;
            int ret = ReadAtCursor(pcursor, ssKey, ssValue);
            if (ret == DB_NOTFOUND)
                break;
            else if (ret != 0)
                return false;

            // Unserialize
            // Taking advantage of the fact that pair serialization
            // is just the two items serialized one after the other,
            // so we can deserialize them one at a time
            string strType;
            // Deserialize the first item in the pair
            ssKey >> strType;

            // IP address:
            if (strType == "name")
            {
                string strAddress;
                // Deserialize the second item in the pair
                ssKey >> strAddress;
                // Load the address into the global address book
                ssValue >> mapAddressBook[strAddress];
            }
            // Money sent to me:
            else if (strType == "tx")
            {
                uint256 hash;
                // Deserialize the second item in the pair
                ssKey >> hash;
                // Load the coins sent to me into the wallet:
                CWalletTx& wtx = mapWallet[hash];
                ssValue >> wtx;

                if (wtx.GetHash() != hash)
                    printf("Error in wallet.dat, hash mismatch\n");

                //// debug print
                //printf("LoadWallet  %s\n", wtx.GetHash().ToString().c_str());
                //printf(" %12I64d  %s  %s  %s\n",
                //    wtx.vout[0].nValue,
                //    DateTimeStr(wtx.nTime).c_str(),
                //    wtx.hashBlock.ToString().substr(0,14).c_str(),
                //    wtx.mapValue["message"].c_str());
            }
            // A bitcoin public/private key pair that I own:
            else if (strType == "key")
            {
                vector<unsigned char> vchPubKey;
                // Deserialize the second item in the pair
                ssKey >> vchPubKey;
                CPrivKey vchPrivKey;
                ssValue >> vchPrivKey;

                // Load the keys i own into memory:
                mapKeys[vchPubKey] = vchPrivKey;
                mapPubKeys[Hash160(vchPubKey)] = vchPubKey;
            }
            // The public/private key I'll use to send myself spare change:
            else if (strType == "defaultkey")
            {
                ssValue >> vchDefaultKeyRet;
            }
            // Settings
            else if (strType == "setting")  /// or settings or option or options or config?
            {
                string strKey;
                // Deserialize the second item in the pair
                ssKey >> strKey;
                // Load settings into global state:
                if (strKey == "fGenerateBitcoins")  ssValue >> fGenerateBitcoins;
                if (strKey == "nTransactionFee")    ssValue >> nTransactionFee;
                if (strKey == "addrIncoming")       ssValue >> addrIncoming;
            }
        }
    }

    printf("fGenerateBitcoins = %d\n", fGenerateBitcoins);
    printf("nTransactionFee = %I64d\n", nTransactionFee);
    printf("addrIncoming = %s\n", addrIncoming.ToString().c_str());

    return true;
}

bool LoadWallet()
{
    vector<unsigned char> vchDefaultKey;
    if (!CWalletDB("cr").LoadWallet(vchDefaultKey))
        return false;

    if (mapKeys.count(vchDefaultKey))
    {
        // Set keyUser
        keyUser.SetPubKey(vchDefaultKey);
        keyUser.SetPrivKey(mapKeys[vchDefaultKey]);
    }
    else
    {
        // Create new keyUser and set as default key
        RandAddSeed(true);
        keyUser.MakeNewKey();
        if (!AddKey(keyUser))
            return false;
        if (!SetAddressBookName(PubKeyToAddress(keyUser.GetPubKey()), "Your Address"))
            return false;
        CWalletDB().WriteDefaultKey(keyUser.GetPubKey());
    }

    return true;
}
