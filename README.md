Luigi jsm
=========

[Luigi](https://github.com/spotify/luigi)と[jsm](https://github.com/utahta/jsm)を使って株価を収集するワークフローを構築する


### Luigi

バッチジョブのパイプラインの構築するモジュール  


### jsm

日本の株価を取得するモジュール



### 銘柄を取得してCSVで保存する


```
vagrant@vagrant-ubuntu-trusty-64:/vagrant$ pwd
/vagrant

vagrant@vagrant-ubuntu-trusty-64:/vagrant$ PYTHONPATH='' luigi --module luigi_jsm.tasks ExtractBrandTask --local-scheduler

...

INFO:
===== Luigi Execution Summary =====

Scheduled 1 tasks of which:
* 1 ran successfully:
    - 1 ExtractBrandTask()

This progress looks :) because there were no failed tasks or missing external dependencies

===== Luigi Execution Summary =====

INFO:luigi-interface:
===== Luigi Execution Summary =====

Scheduled 1 tasks of which:
* 1 ran successfully:
    - 1 ExtractBrandTask()

This progress looks :) because there were no failed tasks or missing external dependencies

===== Luigi Execution Summary =====


vagrant@vagrant-ubuntu-trusty-64:/vagrant$ PYTHONPATH='' luigi --module luigi_jsm.tasks ExtractBrandTask --local-scheduler

...

INFO:
===== Luigi Execution Summary =====

Scheduled 1 tasks of which:
* 1 present dependencies were encountered:
    - 1 ExtractBrandTask()

Did not run any tasks
This progress looks :) because there were no failed tasks or missing external dependencies

===== Luigi Execution Summary =====

```