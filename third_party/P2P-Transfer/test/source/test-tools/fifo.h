/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FIFO_H
#define FIFO_H

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <string>

class Fifo {
public:
    Fifo(const std::string &path, __mode_t mode) : path_(path)
    {
        if (mkfifo(path_.c_str(), mode) == -1) {
            throw std::runtime_error("Failed to create FIFO");
        }
    }

    std::string GetPath()
    {
        return path_;
    }

    ~Fifo()
    {
        unlink(path_.c_str());
    }

    int GetFd() const
    {
        return fd_;
    }

private:
    std::string path_;
    int fd_;
};

#endif  // FIFO_H