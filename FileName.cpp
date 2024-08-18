#include <string>
#include <fstream>
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <chrono>
#include <future>
#include <vector>
#include <thread>

using namespace std;

#define PART_SIZE 500000
#define RAND_ARR_SIZE 20000000 

size_t max_threads = thread::hardware_concurrency() - 1;

void merge(int* arr, int* temp_arr, int left, int mid, int right) {
    int i = left, j = mid + 1, k = left;

    while (i <= mid && j <= right) {
        if (arr[i] <= arr[j])
            temp_arr[k++] = arr[i++];
        else
            temp_arr[k++] = arr[j++];
    }

    while (i <= mid)
        temp_arr[k++] = arr[i++];

    while (j <= right)
        temp_arr[k++] = arr[j++];

    for (int i = left; i <= right; i++)
        arr[i] = temp_arr[i];
}

void merge_sort(int* arr, int* temp_arr, int left, int right) {
    if (left >= right)
        return;

    int mid = left + (right - left) / 2;

    if (max_threads > 0) {
        max_threads -= 2;

        auto left_future = async(launch::async, merge_sort, arr, temp_arr, left, mid);
        auto right_future = async(launch::async, merge_sort, arr, temp_arr, mid + 1, right);

        left_future.get();
        right_future.get();

        max_threads += 2;
    }
    else {
        merge_sort(arr, temp_arr, left, mid);
        merge_sort(arr, temp_arr, mid + 1, right);
    }

    merge(arr, temp_arr, left, mid, right);
}

void merge_to_file(const int* arr1, const int* arr2, int sz1, int sz2) {
    fstream temp;
    temp.open("temp_1_file.txt", fstream::out | std::ofstream::trunc);

    const int* first;
    const int* second;

    if (arr1[0] < arr2[0]) {
        first = arr1;
        second = arr2;
    }
    else {
        first = arr2;
        second = arr1;
        swap(sz1, sz2); 
    }

    if (temp.is_open()) {
        int i = 0, j = 0;

        while (i < sz1 && j < sz2) {
            if (first[i] < second[j])
                temp << first[i++] << ' ';
            else if (first[i] == second[j]) {
                temp << first[i++] << ' ';
                temp << second[j++] << ' ';
            }
            else
                temp << second[j++] << ' ';
        }

        while (i < sz1)
            temp << first[i++] << ' ';

        while (j < sz2)
            temp << second[j++] << ' ';

        temp.close();
    }
}

void merge_files() {
    fstream res;
    fstream temp1;
    fstream temp2;

    temp1.open("temp_1_file.txt", fstream::in);
    res.open("res_file.txt", fstream::in);
    temp2.open("temp_2_file.txt", fstream::out | ofstream::trunc);

    if (!temp1.is_open() || !temp2.is_open() || !res.is_open())
        return;

    int temp1_value, res_value;
    temp1 >> temp1_value;
    res >> res_value;

    while (!temp1.eof() && !res.eof()) {
        if (temp1_value <= res_value) {
            temp2 << temp1_value << ' ';
            temp1 >> temp1_value;
        }
        else {
            temp2 << res_value << ' ';
            res >> res_value;
        }
    }

    while (!res.eof()) {
        temp2 << res_value << ' ';
        res >> res_value;
    }

    while (!temp1.eof()) {
        temp2 << temp1_value << ' ';
        temp1 >> temp1_value;
    }

    temp1.close();
    temp2.close();
    res.close();

    res.open("res_file.txt", std::ofstream::out | std::ofstream::trunc);
    if (res.is_open())
        res.close();

    filesystem::copy_file("temp_2_file.txt", "res_file.txt", filesystem::copy_options::overwrite_existing);
}

int read_part_arr(fstream& fs, int*& arr) {
    arr = new int[PART_SIZE];
    int* tmp_arr;
    int i;

    for (i = 0; i < PART_SIZE && !fs.eof(); i++)
        fs >> arr[i];

    if (i == 1) {
        delete[] arr;
        return 0;
    }

    if (i != PART_SIZE) {
        tmp_arr = new int[i];
        for (size_t j = 0; j < i; j++)
            tmp_arr[j] = arr[j];

        delete[] arr;
        arr = tmp_arr;
        return i - 1;
    }

    return PART_SIZE;
}

void sort_func(const string& filename) {
    fstream fs;
    fs.open(filename, fstream::in);

    if (fs.is_open()) {
        while (!fs.eof()) {
            int* part_1;
            int* part_2;

            int size_1 = read_part_arr(fs, part_1);
            int size_2 = read_part_arr(fs, part_2);
            if (size_1 == 0 || size_2 == 0)
                return;

            int* temp_arr1 = new int[size_1];
            int* temp_arr2 = new int[size_2];

            auto handle1 = async(launch::async, merge_sort, part_1, temp_arr1, 0, size_1 - 1);
            auto handle2 = async(launch::async, merge_sort, part_2, temp_arr2, 0, size_2 - 1);

            handle1.get();
            handle2.get();

            merge_to_file(part_1, part_2, size_1, size_2);
            merge_files();

            delete[] temp_arr1;
            delete[] temp_arr2;
            delete[] part_1;
            delete[] part_2;
        }
        fs.close();
    }
}

void write_rand_arr(const string& filename) {
    fstream fs;

    srand(time(nullptr));
    int lef_border = -100;
    int range_len = 50000;

    fs.open(filename, fstream::out | ofstream::trunc);
    if (fs.is_open()) {
        for (int i = 0; i < RAND_ARR_SIZE; i++)
            fs << (lef_border + rand() % range_len) << ' ';

        fs.close();
    }
}

int main(int argc, char const* argv[]) {
    string filename = "array_data.txt";

    write_rand_arr(filename);
    cout << "Generating data is done!" << endl;

    fstream res;
    res.open("res_file.txt", fstream::out | ofstream::trunc);
    res.close();

    auto start = chrono::high_resolution_clock::now();
    sort_func(filename);
    auto finish = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = finish - start;
    cout << "Elapsed time: " << elapsed.count() << " sec" << endl;

    return 0;
}