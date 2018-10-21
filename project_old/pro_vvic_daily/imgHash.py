import cv2
import  numpy as np
import urllib
import time

#均值哈希算法
def aHash(img):
    #缩放为8*8
    img=cv2.resize(img,(8,8),interpolation=cv2.INTER_CUBIC)
    #转换为灰度图
    gray=cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)
    #s为像素和初值为0，hash_str为hash值初值为''
    s=0
    hash_str=''
    #遍历累加求像素和
    for i in range(8):
        for j in range(8):
            s=s+gray[i,j]
    #求平均灰度
    avg=s/64
    #灰度大于平均值为1相反为0生成图片的hash值
    for i in range(8):
        for j in range(8):
            if  gray[i,j]>avg:
                hash_str=hash_str+'1'
            else:
                hash_str=hash_str+'0'
    return hash_str

#差值感知算法
def dHash(img):
    #缩放8*8
    img=cv2.resize(img,(9,8),interpolation=cv2.INTER_CUBIC)
    #转换灰度图
    gray=cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)
    hash_str=''
    #每行前一个像素大于后一个像素为1，相反为0，生成哈希
    for i in range(8):
        for j in range(8):
            if   gray[i,j]>gray[i,j+1]:
                hash_str=hash_str+'1'
            else:
                hash_str=hash_str+'0'
    return hash_str

#Hash值对比
def cmpHash(hash1,hash2):
    n=0
    #hash长度不同则返回-1代表传参出错
    if len(hash1)!=len(hash2):
        return -1
    #遍历判断
    for i in range(len(hash1)):
        #不相等则n计数+1，n最终为相似度
        if hash1[i]!=hash2[i]:
            n=n+1
    return n

def get_hash(urls):
    hashes = []

    for url in urls:
        try:
            resp = urllib.request.urlopen(url)
        except:
            continue
        images = np.asarray(bytearray(resp.read()), dtype="uint8")
        imagea = cv2.imdecode(images, cv2.IMREAD_COLOR)
        try:
            hash = aHash(imagea)
            hashes.append(hash)
        except:
            pass
            stop = 0

        # imgs1.append(image)
        # cv2.imwrite('1.jpg', image)
        # cv2.imshow("Image", image)
        # cv2.waitKey(0)
    return  hashes




if __name__ == '__main__':
    t1 = time.time()
    img1=cv2.imread('1.jpg')
    img2=cv2.imread('2.jpg')

    hash1= aHash(img1)
    hash2= aHash(img2)

    # print(hash1)
    # print(hash2)
    n=cmpHash(hash1,hash2)
    t2 = time.time()
    tt = t2-t1


    print('均值哈希算法相似度：',n)


    hash1= dHash(img1)
    hash2= dHash(img2)
    print(hash1)
    print(hash2)
    n=cmpHash(hash1,hash2)
    print('差值哈希算法相似度：',n)