## firebase functions
### 概要
iOSアプリ（fifth-projectリポジトリ）のバックグラウンド全般  
firestoreと連携
### 機能一覧
- extractTargets・・・  
新規質問が登録されたことをトリガーに、回答を問い合わせるターゲットをランダム抽出し、firestoreに登録  
- aggregate・・・  
時間制限を過ぎた質問を抽出し、それぞれ非同期に集計し、更新すべきドキュメントを非同期に更新  
- pushAskingToTargets・・・  
新規質問の回答者として抽出されたことをトリガーに、対象回答者に対しプッシュ通知  
- pushResultTargets・・・  
集計が完了したことをトリガーに、質問の回答者に対しプッシュ通知  
- pushResultToOwners・・・  
集計が完了したことをトリガーに、質問の質問者に対しプッシュ通知  
- deleteTargets・・・  
集計結果をユーザが受信したことをトリガーに、target（質問に対し回答をしたユーザ）ドキュメントを削除  
- deleteAnswer・・・  
回答の集計が完了したことをトリガーに、質問に対する回答ドキュメントを削除   
- DBトランザクション処理  
- ロギング  
