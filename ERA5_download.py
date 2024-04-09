import time
import cdsapi
import requests
import multiprocessing

c = cdsapi.Client()  # timeout=300
years = [str(id1) for id1 in range(2015, 2023)]
months = ['%02d' % id2 for id2 in range(1, 13)]

# 下载的数据类型
stat = "daily_mean"
vars = ['2m_temperature']


def Download(iyear, imonth, var):
    t000 = time.time()
    result = c.service(
        "tool.toolbox.orchestrator.workflow",
        params={

            "realm": "c3s",
            "project": "app-c3s-daily-era5-statistics",
            "version": "master",
            "kwargs": {
                "dataset": "reanalysis-era5-land",
                "product_type": "reanalysis",
                "variable": var,
                "statistic": stat,
                "year": iyear,
                "month": imonth,
                "time_zone": "UTC+00:0",
                "frequency": "1-hourly",
                "grid": "0.25/0.25",
                "area": {
                    "lat": [23.75, 55.25],
                    "lon": [85, 116.5]
                }
            },
            "workflow_name": "application"
        }
    )

    file_name = r"E:\\data\\label\\" + stat + "_" + var + iyear + imonth + ".nc"

    location = result[0]['location']
    res = requests.get(location, stream=True)
    print("Writing data to " + file_name)
    with open(file_name, 'wb') as fh:
        for r in res.iter_content(chunk_size=1024):
            fh.write(r)
    fh.close()
    print('***样本%s 保存结束, 耗时: %.3f s / %.3f mins****************' % (
        file_name, (time.time() - t000), (time.time() - t000) / 60))


if __name__ == "__main__":
    t0 = time.time()
    print('*****************程序开始*********************')
    for v in vars:

        for yr in years:
            for mn in months:
                Download(yr, mn, v)
    print('***********************程序结束, 耗时: %.3f s / %.3f mins****************' % (
        (time.time() - t0), (time.time() - t0) / 60))





