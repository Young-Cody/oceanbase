#include <iostream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <random>
using namespace std;

int64_t ls_num;
int64_t part_num;
vector<vector<int64_t>> ls_parts_data;
vector<int64_t> ls_data_size_;
vector<int64_t> part_size_segment = {10, 5, 2, 1};


void init()
{
    cout << "----------init----------" << endl;
    std::random_device rd;
    srand((unsigned int)time(nullptr));
    ls_num = rd() % 20 + 2;
    ls_parts_data.clear();
    ls_data_size_.clear();
    ls_parts_data.resize(ls_num);
    ls_data_size_.resize(ls_num, 0);
    part_num = rd() % 1000 + 1;
    for (int64_t i = 0; i < ls_num; i++) {
        ls_parts_data[i].resize(part_num);
        for (int64_t j = 0; j < part_num; j++) {
            ls_parts_data[i][j] = rd() % (1000 + i * 100);
            ls_data_size_[i] += ls_parts_data[i][j];
        }
    }
    auto ls_max_it = std::max_element(ls_data_size_.begin(), ls_data_size_.end());
    auto ls_min_it = std::min_element(ls_data_size_.begin(), ls_data_size_.end());
    int64_t ls_max_size = *ls_max_it;
    int64_t ls_min_size = *ls_min_it;
    cout << "ls_num: " << ls_num << " part_num: " << ls_num * part_num << " ls_max_size: " << ls_max_size << " ls_min_size: " << ls_min_size << " relative diff:" << ((double)ls_max_size - ls_min_size) / ls_max_size << endl << endl;
}

int64_t origin_swap(vector<vector<int64_t>> &ls_parts, vector<int64_t> &ls_data_size,
        size_t src_ls_idx, size_t dst_ls_idx, int64_t swap_min_size, int64_t ls_delta)
{
    vector<int64_t> &src_ls = ls_parts[src_ls_idx];
    vector<int64_t> &dst_ls = ls_parts[dst_ls_idx];
    sort(src_ls.begin(), src_ls.end());
    sort(dst_ls.begin(), dst_ls.end());
    int64_t swap_size = src_ls.back() - dst_ls.front();
    if (swap_min_size <= swap_size && swap_size < ls_delta) {
        ls_data_size[src_ls_idx] += dst_ls.front() - src_ls.back();
        ls_data_size[dst_ls_idx] += src_ls.back() - dst_ls.front();
        swap(src_ls.back(), dst_ls.front());
        return 1;
    }
    return 0;
}

int64_t optimize_swap(vector<vector<int64_t>> &ls_parts, vector<int64_t> &ls_data_size,
        size_t src_ls_idx, size_t dst_ls_idx, int64_t swap_min_size, int64_t ls_delta)
{
    vector<int64_t> &src_ls = ls_parts[src_ls_idx];
    vector<int64_t> &dst_ls = ls_parts[dst_ls_idx];
    sort(src_ls.begin(), src_ls.end());
    sort(dst_ls.begin(), dst_ls.end());
    int64_t src_idx = 0;
    int64_t swap_size = 0;
    for (int dst_idx = 0; dst_idx < dst_ls.size() && src_idx < src_ls.size(); dst_idx++) {
        swap_size = src_ls[src_idx] - dst_ls[dst_idx];
        while (src_idx < src_ls.size() && swap_size < swap_min_size) {
            swap_size = src_ls[src_idx] - dst_ls[dst_idx];
            src_idx++;
        }
        if (swap_size < ls_delta) {
            ls_data_size[src_ls_idx] += dst_ls[dst_idx] - src_ls[src_idx];
            ls_data_size[dst_ls_idx] += src_ls[src_idx] - dst_ls[dst_idx];
            swap(src_ls[src_idx], dst_ls[dst_idx]);
            return 1;
        }
    }
    return 0;
}

void origin()
{
    cout << "\033[31m" << "----------before_optimize----------" << "\033[0m" << endl << endl;
    vector<vector<int64_t>> ls_parts = ls_parts_data;
    vector<int64_t> ls_data_size = ls_data_size_;
    vector<int64_t>::iterator ls_max_it, ls_min_it;
    int64_t ls_max_size = 0;
    int64_t ls_min_size = 0;
    auto start = std::chrono::high_resolution_clock::now();
    while (true) {
        ls_max_it = std::max_element(ls_data_size.begin(), ls_data_size.end());
        ls_min_it = std::min_element(ls_data_size.begin(), ls_data_size.end());
        ls_max_size = *ls_max_it;
        ls_min_size = *ls_min_it;
        if (((double)ls_max_size - ls_min_size) / ls_max_size < 0.1) {
            break;
        }
        int64_t balance_cnt = 0;
        for (auto &segment : part_size_segment) {
            balance_cnt = origin_swap(ls_parts, ls_data_size, ls_max_it - ls_data_size.begin(), ls_min_it - ls_data_size.begin(), segment, ls_max_size - ls_min_size);
            if (balance_cnt) {
                break;
            }
        }
        if (balance_cnt == 0) {
            break;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    cout << "time(ms): " << duration.count() / 1000 <<  " ls_max_size: " << ls_max_size << " ls_min_size: " << ls_min_size << " relative diff:" << ((double)ls_max_size - ls_min_size) / ls_max_size << endl << endl;
}

void optimize()
{
    cout << "\033[32m" << "----------after_optimize----------" << "\033[0m" << endl << endl;
    vector<vector<int64_t>> ls_parts = ls_parts_data;
    vector<int64_t> ls_data_size = ls_data_size_;
    vector<int64_t>::iterator ls_max_it, ls_min_it;
    int64_t ls_max_size = 0;
    int64_t ls_min_size = 0;
    auto start = std::chrono::high_resolution_clock::now();
    while (true) {
        ls_max_it = std::max_element(ls_data_size.begin(), ls_data_size.end());
        ls_min_it = std::min_element(ls_data_size.begin(), ls_data_size.end());
        ls_max_size = *ls_max_it;
        ls_min_size = *ls_min_it;
        if (((double)ls_max_size - ls_min_size) / ls_max_size < 0.1) {
            break;
        }
        int64_t balance_cnt = 0;
        for (auto &segment : part_size_segment) {
            balance_cnt = optimize_swap(ls_parts, ls_data_size, ls_max_it - ls_data_size.begin(), ls_min_it - ls_data_size.begin(), segment, ls_max_size - ls_min_size);
            if (balance_cnt) {
                break;
            }
        }
        if (balance_cnt == 0) {
            break;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    cout << "time(ms): " << duration.count() / 1000 <<  " ls_max_size: " << ls_max_size << " ls_min_size: " << ls_min_size << " relative diff:" << ((double)ls_max_size - ls_min_size) / ls_max_size << endl << endl;
}

void test() {
    int64_t test_num = 100;
    while (test_num--) {
        init();
        origin();
        optimize();
    }
}

int main() {
    test();
    return 0;
}
