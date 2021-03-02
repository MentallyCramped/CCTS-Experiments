#include <iostream>
#include <random>
#include <vector>
#include <unordered_map>
#include <thread>
#include <fstream>
#include <mutex>
#include <chrono>
#include <shared_mutex>
#include <set>
using namespace std;

typedef long long ll;
typedef vector<ll> vll;
typedef vector< vector<ll>> vvll;
typedef set<ll> sll;
vector<thread> threads;
ll max_threads = thread::hardware_concurrency(), tcount = 0, num_vars = 0, commit_time = 0;
mutex tcount_l, printl, valwritel, statl;
shared_mutex live_set_l;
vector<shared_mutex> var_lock, gwritel;      //lock for global memory and gwrite
vll live_set, var_value, committed;
vvll gwrite, gread;
auto begin_time = chrono::steady_clock::now();
unordered_map<ll, ll> timestamp;

string gettime(){
    time_t t = time(NULL);
    return ctime(&t);
}

ll read(ll var, ll tid, unordered_map<ll,ll> &ts){
    var_lock[var].lock_shared();
    ts[var] = (chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - begin_time).count());
//    gread[var].push_back(tid);
    ll val = var_value[var];
    //Print Locks are unnecessary if the algorithm works
//    printl.lock();
//    cout << "Trans " << tid <<" reads " << var << " with value " << val << " at time " << gettime() << endl;
//    printl.unlock();
    var_lock[var].unlock_shared();
    return val;
}

bool intersect(vll a, vll b, ll rvar, unordered_map<ll,ll> &ts){
    if(a.size() < b.size()) swap(a,b);
    unordered_map<ll, ll> ht;
    for(int i = 0; i < a.size(); i++){
        ht[a[i]] = 1;
    }
    for(int i = 0; i < b.size(); i++){
        if(ht.find(b[i]) != ht.end()) {
            if(timestamp[b[i]] > ts[rvar]) return true;
        }
    }
    return false;
}

/*
 * each tid should have their commit time
 */

bool val_write(const sll &RS, const sll &WS, ll tid, vll live_set_begin, unordered_map<ll,ll> &ts){
    valwritel.lock();
//    printl.lock();
//    cout << "Trans " << tid <<" enters val write at time " << gettime() << endl;
//    printl.unlock();
    bool abort = false;
    for(auto it = RS.begin(); it != RS.end(); it++){
        ll rvar = *it;
        gwritel[rvar].lock_shared();
        if(intersect(live_set_begin, gwrite[rvar], rvar, ts)) {
            abort = true;
            gwritel[rvar].unlock_shared();
            break;
        }
        gwritel[rvar].unlock_shared();
    }
    if(!abort){
        for(auto it = WS.begin(); it != WS.end(); it++){
            ll wvar = *it;
            var_lock[wvar].lock();
            gwritel[wvar].lock();
            gwrite[wvar].push_back(tid);    //update gwrite
            var_value[wvar] = tid;  //write random value
//            printl.lock();
//            cout << "Trans " << tid <<" writes " << wvar << " with value " << tid << " at time " << gettime() << endl;
//            printl.unlock();
            gwritel[wvar].unlock();
            var_lock[wvar].unlock();
        }
    }
    valwritel.unlock();
    return abort;
}

ll func(ll tid, string trans){
    unordered_map<ll, ll> ts;
//    printl.lock();
//    cout << "Trans " << tid << " has started at time " << gettime() << endl;
//    printl.unlock();
    auto start = chrono::steady_clock::now();
    while(true) {
        live_set_l.lock_shared();
        vll live_set_begin(live_set);
        live_set_l.unlock_shared();
        vll localvar(num_vars, INT16_MIN);
        sll RS, WS;
        int i = 0;
        while (i < trans.size()) {
            char op = trans[i];
            i++;
            ll var = 0;
            while (trans[i] != ',' && trans[i] != '.') {
                var = var * 10 + (trans[i] - '0');
                i++;
            }
            i++;
            if (op == 'r') {
                if (RS.contains(var)) break;
                RS.insert(var);
                localvar[var] = read(var, tid, ts);
            } else {
                WS.insert(var);
                localvar[var] = INT16_MIN;
            }
        }
        bool abort = val_write(RS, WS, tid, live_set_begin, ts);
        if(!abort){
//            printl.lock();
//            cout << "Trans " << tid << " has committed at time " << gettime();
//            printl.unlock();
            break;
        }
        else{
//            printl.lock();
//            cout << "Trans " << tid << " has aborted at time " << gettime() << ". Restarting thread!" << endl;
//            printl.unlock();
        }
    }
    auto end = chrono::steady_clock::now();
    timestamp[tid] = (chrono::duration_cast<chrono::microseconds>(end - begin_time).count());
    statl.lock();
    commit_time += chrono::duration_cast<chrono::microseconds>(end - start).count();
    statl.unlock();
    live_set_l.lock();
    live_set.erase(lower_bound(live_set.begin(), live_set.end(), tid));
    live_set_l.unlock();
    tcount_l.lock();
    tcount--;
    tcount_l.unlock();
    //handle gc signals
}

int main(){
    //Init code
    fstream in;
    in.open("C:\\cygwin64\\CCTS_reimpl\\random_test.txt", ios::in);
    ll tid= 0;
    string temp; in >> temp;
    num_vars = stoll(temp);
    for(int i = 0; i < num_vars; i++) {
        gwrite.push_back({});   //{} is an empty vector
        gread.push_back({});
        var_value.push_back(0);
    }
    vector<shared_mutex> empty(num_vars), empty2(num_vars);
    var_lock.swap(empty);
    gwritel.swap(empty2);


    //Reading all the transactions
    while(!in.eof()) {
        while (tcount == max_threads);    //we haven't used locks here, but it works in reality
        tid++;
        tcount_l.lock();
        tcount++;
        tcount_l.unlock();
        string trans;
        in >> trans;
        live_set_l.lock();
        live_set.push_back(tid);
        live_set_l.unlock();
        thread t(func, tid, trans);
        t.detach();
    }
    in.close();
    while(tcount > 0);
    cout << "All threads have terminated! Average commit time in millisecs " << commit_time/(tid*1000) << endl;
}